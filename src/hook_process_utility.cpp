/*
 * hook_process_utility.cpp - ProcessUtility Hook 实现（延迟同步模式）
 *
 * 功能：
 * - CREATE TABLE：注册 Delta 表映射，不创建外表
 * - ALTER TABLE：记录 DDL 变化到日志表，不立即同步
 * - DROP TABLE：删除映射和日志，同时删除 Iceberg 外表（如果已创建）
 */

#include "../include/dual_table.h"

/* ==================== 判断函数 ==================== */

static bool
should_create_delta_table(CreateStmt *stmt)
{
    /* 检查是否是临时表 */
    if (stmt->relation->relpersistence == RELPERSISTENCE_TEMP)
        return false;

    /* 检查是否是系统表 */
    if (stmt->relation->schemaname &&
        (strcmp(stmt->relation->schemaname, "pg_catalog") == 0 ||
         strcmp(stmt->relation->schemaname, "information_schema") == 0))
        return false;

    /* 检查是否有禁用标记 */
    ListCell *cell;
    foreach(cell, stmt->options)
    {
        DefElem *def = (DefElem *) lfirst(cell);
        if (strcmp(def->defname, "delta_table_sync") == 0)
        {
            char *value = defGetString(def);
            if (strcmp(value, "off") == 0 || strcmp(value, "disable") == 0)
                return false;
        }
    }

    return true;
}

/* ==================== Hook 主函数 ==================== */

void
delta_table_process_utility_hook(
    processutility_context *cxt,
    DestReceiver *dest,
    bool sentToRemote,
    char *completionTag,
    ProcessUtilityContext context,
    bool isCTAS)
{
    Node *parse_tree = cxt->parse_tree;
    NodeTag node_tag = nodeTag(parse_tree);

    /* ========== COPY FROM ========== */
    if (node_tag == T_CopyStmt)
    {
        CopyStmt *stmt = (CopyStmt *) parse_tree;

        if (stmt->is_from && stmt->relation)
        {
            /* 打开目标表检查是否是 Delta 表 */
            LOCKMODE lockmode = RowExclusiveLock;
            Relation rel = table_openrv(stmt->relation, lockmode);
            Oid relid = RelationGetRelid(rel);

            const char *location = get_iceberg_location(relid);

            if (location != NULL)
            {
                /* 是 Delta 表，执行批量 COPY FROM */
                ParseState *pstate = make_parsestate(NULL);
                pstate->p_sourcetext = cxt->query_string;

                ProcessDeltaTableCopyFrom(stmt, rel, pstate, completionTag);

                pfree(pstate);
                table_close(rel, NoLock);
                return;  /* 拦截完成，不调用标准流程 */
            }

            /* 非 Delta 表，继续标准流程 */
            table_close(rel, lockmode);
        }
    }

    /* ========== CREATE TABLE ========== */
    if (node_tag == T_CreateStmt)
    {
        CreateStmt *stmt = (CreateStmt *) parse_tree;

        /* 先执行标准流程创建 Delta 内表 */
        if (ProcessUtility_hook && ProcessUtility_hook != delta_table_process_utility_hook)
            ProcessUtility_hook(cxt, dest, sentToRemote, completionTag, context, isCTAS);
        else
            standard_ProcessUtility(cxt, dest, sentToRemote, completionTag, context, isCTAS);

        /* 注册 Delta 表（不创建外表，等到 FLUSH 时创建） */
        if (should_create_delta_table(stmt))
        {
            Oid namespace_oid = RangeVarGetCreationNamespace(stmt->relation);
            Oid delta_relid = get_relname_relid(stmt->relation->relname, namespace_oid);

            if (delta_relid != InvalidOid)
            {
                const char *namespace_name = get_namespace_name_safe(namespace_oid);
                char *location = generate_iceberg_location(namespace_name, stmt->relation->relname);

                /* 注册映射（iceberg_table_oid 为 NULL） */
                register_delta_table_mapping(delta_relid, location);

                elog(LOG, "Delta Table: Registered delta table %s.%s, Iceberg table will be created on FLUSH",
                     namespace_name, stmt->relation->relname);

                pfree(location);
            }
        }

        return;
    }

    /* ========== ALTER TABLE ========== */
    if (node_tag == T_AlterTableStmt)
    {
        AlterTableStmt *stmt = (AlterTableStmt *) parse_tree;

        /* 先执行内表 ALTER */
        if (ProcessUtility_hook && ProcessUtility_hook != delta_table_process_utility_hook)
            ProcessUtility_hook(cxt, dest, sentToRemote, completionTag, context, isCTAS);
        else
            standard_ProcessUtility(cxt, dest, sentToRemote, completionTag, context, isCTAS);

        /* 记录 DDL 变化到日志表（延迟同步） */
        if (stmt->relkind == OBJECT_TABLE)
        {
            Oid relid = RangeVarGetRelid(stmt->relation, AccessShareLock, true);
            if (relid != InvalidOid)
            {
                /* 检查是否是 Delta 表 */
                const char *location = get_iceberg_location(relid);
                if (location != NULL)
                {
                    /* 遍历 ALTER 子命令，记录到日志 */
                    ListCell *cell;
                    foreach(cell, stmt->cmds)
                    {
                        AlterTableCmd *cmd = (AlterTableCmd *) lfirst(cell);

                        switch (cmd->subtype)
                        {
                            case AT_AddColumn:
                            case AT_AddColumnRecurse:
                                {
                                    ColumnDef *colDef = (ColumnDef *) cmd->def;
                                    Oid typoid;
                                    int32 typmod = 0;
                                    typenameTypeIdAndMod(NULL, colDef->typname, &typoid, &typmod, NULL);

                                    char *type_str = format_type_with_typemod(typoid, typmod);

                                    log_ddl_change(relid, "ADD_COLUMN",
                                        colDef->colname, type_str, NULL, colDef->is_not_null);

                                    elog(LOG, "Delta Table: Logged ADD COLUMN %s %s on delta table %u",
                                         colDef->colname, type_str, relid);
                                }
                                break;

                            case AT_DropColumn:
                            case AT_DropColumnRecurse:
                                log_ddl_change(relid, "DROP_COLUMN",
                                    cmd->name, NULL, NULL, false);

                                elog(LOG, "Delta Table: Logged DROP COLUMN %s on delta table %u",
                                     cmd->name, relid);
                                break;

                            case AT_AlterColumnType:
                                {
                                    TypeName *typeName = (TypeName *) cmd->def;
                                    Oid typoid;
                                    int32 typmod = 0;
                                    typenameTypeIdAndMod(NULL, typeName, &typoid, &typmod, NULL);

                                    char *new_type_str = format_type_with_typemod(typoid, typmod);

                                    log_ddl_change(relid, "ALTER_TYPE",
                                        cmd->name, NULL, new_type_str, false);

                                    elog(LOG, "Delta Table: Logged ALTER COLUMN %s TYPE %s on delta table %u",
                                         cmd->name, new_type_str, relid);
                                }
                                break;

                            case AT_SetNotNull:
                                log_ddl_change(relid, "SET_NOT_NULL",
                                    cmd->name, NULL, NULL, true);

                                elog(LOG, "Delta Table: Logged SET NOT NULL on column %s, delta table %u",
                                     cmd->name, relid);
                                break;

                            case AT_DropNotNull:
                                log_ddl_change(relid, "DROP_NOT_NULL",
                                    cmd->name, NULL, NULL, false);

                                elog(LOG, "Delta Table: Logged DROP NOT NULL on column %s, delta table %u",
                                     cmd->name, relid);
                                break;

                            case AT_ColumnDefault:
                                /* DEFAULT 值处理 */
                                if (cmd->def)
                                {
                                    char *default_str = deparse_expression((Node *) cmd->def, NULL, false, false);
                                    log_ddl_change(relid, "SET_DEFAULT",
                                        cmd->name, default_str, NULL, false);
                                    pfree(default_str);
                                }
                                else
                                {
                                    log_ddl_change(relid, "DROP_DEFAULT",
                                        cmd->name, NULL, NULL, false);
                                }
                                break;

                            default:
                                /* 其他 DDL 类型暂不记录 */
                                elog(DEBUG1, "Delta Table: ALTER subtype %d not logged for delayed sync",
                                     cmd->subtype);
                                break;
                        }
                    }
                }
            }
        }

        return;
    }

    /* ========== DROP TABLE ========== */
    if (node_tag == T_DropStmt)
    {
        DropStmt *stmt = (DropStmt *) parse_tree;

        if (stmt->removeType == OBJECT_TABLE)
        {
            ListCell *cell;

            /* 先检查并删除 Iceberg 外表和映射 */
            foreach(cell, stmt->objects)
            {
                RangeVar *rel_var = (RangeVar *) lfirst(cell);
                Oid relid = RangeVarGetRelid(rel_var, AccessShareLock, true);

                if (relid != InvalidOid)
                {
                    /* 检查是否是 Delta 表 */
                    Oid iceberg_relid = get_iceberg_table_oid(relid);
                    const char *location = get_iceberg_location(relid);

                    if (location != NULL)
                    {
                        /* 如果 Iceberg 外表已创建，先删除它 */
                        if (iceberg_relid != InvalidOid)
                        {
                            StringInfoData drop_sql;
                            initStringInfo(&drop_sql);

                            appendStringInfo(&drop_sql, "DROP FOREIGN TABLE %s.%s",
                                get_namespace_name_safe(get_rel_namespace(iceberg_relid)),
                                get_rel_name(iceberg_relid));

                            SPI_CONNECT_COMPAT();
                            SPI_EXECUTE_COMPAT(drop_sql.data, false, 0);
                            SPI_FINISH_COMPAT();

                            elog(LOG, "Delta Table: Dropped Iceberg foreign table for %s", rel_var->relname);
                            pfree(drop_sql.data);
                        }

                        /* 清理数据文件 */
                        if (location)
                        {
                            StringInfoData cleanup_cmd;
                            initStringInfo(&cleanup_cmd);
                            appendStringInfo(&cleanup_cmd, "rm -rf %s", location);
                            system(cleanup_cmd.data);
                            pfree(cleanup_cmd.data);
                        }

                        /* 删除映射和 DDL 日志 */
                        remove_delta_table_mapping(relid);
                    }
                }
            }

            /* 执行标准 DROP（删除内表） */
            if (ProcessUtility_hook && ProcessUtility_hook != delta_table_process_utility_hook)
                ProcessUtility_hook(cxt, dest, sentToRemote, completionTag, context, isCTAS);
            else
                standard_ProcessUtility(cxt, dest, sentToRemote, completionTag, context, isCTAS);

            return;
        }
    }

    /* ========== TRUNCATE TABLE ========== */
    if (node_tag == T_TruncateStmt)
    {
        TruncateStmt *stmt = (TruncateStmt *) parse_tree;

        /* 先执行标准 TRUNCATE */
        if (ProcessUtility_hook && ProcessUtility_hook != delta_table_process_utility_hook)
            ProcessUtility_hook(cxt, dest, sentToRemote, completionTag, context, isCTAS);
        else
            standard_ProcessUtility(cxt, dest, sentToRemote, completionTag, context, isCTAS);

        /* TRUNCATE 后标记有待刷新的删除操作 */
        ListCell *cell;
        foreach(cell, stmt->relations)
        {
            RangeVar *rel_var = (RangeVar *) lfirst(cell);
            Oid relid = RangeVarGetRelid(rel_var, AccessShareLock, true);

            if (relid != InvalidOid)
            {
                const char *location = get_iceberg_location(relid);
                if (location != NULL)
                {
                    /* 记录 TRUNCATE 操作 */
                    log_ddl_change(relid, "TRUNCATE", NULL, NULL, NULL, false);

                    elog(LOG, "Delta Table: Logged TRUNCATE on delta table %u", relid);
                }
            }
        }

        return;
    }

    /* 其他命令：标准处理 */
    if (ProcessUtility_hook && ProcessUtility_hook != delta_table_process_utility_hook)
        ProcessUtility_hook(cxt, dest, sentToRemote, completionTag, context, isCTAS);
    else
        standard_ProcessUtility(cxt, dest, sentToRemote, completionTag, context, isCTAS);
}