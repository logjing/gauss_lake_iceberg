/*
 * catalog_manager.cpp - Delta/Iceberg Catalog 映射管理（延迟同步模式）
 *
 * 功能：
 * - 注册 Delta 表映射（CREATE TABLE 时）
 * - 记录 DDL 变化到日志表（ALTER TABLE 时，不立即同步）
 * - 查询映射关系
 */

#include "../include/dual_table.h"

/* ==================== Catalog 操作 ==================== */

/*
 * 注册 Delta 表到 gaussvector.delta_tables
 * 注意：此时 iceberg_table_oid 为 NULL，直到首次 FLUSH 才会创建外表
 *
 * PG 包装函数，可从 SQL 调用
 */

/* 使用 C 链接的包装函数 */
extern "C" {
    Datum
    register_delta_table_mapping(PG_FUNCTION_ARGS)
    {
        Oid delta_relid = PG_GETARG_OID(0);
        text *location_text = PG_GETARG_TEXT_P(1);
        char *location = text_to_cstring(location_text);

        register_delta_table_mapping_internal(delta_relid, location);

        pfree(location);
        PG_RETURN_VOID();
    }
}

/* PG_FUNCTION_INFO_V1 必须在 extern "C" 块之外 */
PG_FUNCTION_INFO_V1(register_delta_table_mapping);

/*
 * 内部实现函数
 */
void
register_delta_table_mapping_internal(Oid delta_relid, const char *location)
{
    SPI_CONNECT_COMPAT();

    StringInfoData query;
    initStringInfo(&query);

    appendStringInfo(&query,
        "INSERT INTO gaussvector.delta_tables "
        "(delta_table_oid, iceberg_location, pending_changes) "
        "VALUES (%u, '%s', true) "
        "ON CONFLICT (delta_table_oid) DO UPDATE SET "
        "iceberg_location = '%s', pending_changes = true",
        delta_relid, location, location);

    SPI_EXECUTE_COMPAT(query.data, false, 0);
    pfree(query.data);

    elog(LOG, "Registered delta table %u with location %s", delta_relid, location);

    /* 创建 DML 触发器，用于拦截 INSERT/UPDATE/DELETE 操作 */
    resetStringInfo(&query);
    appendStringInfo(&query,
        "SELECT gaussvector.create_delta_trigger(%u::regclass)",
        delta_relid);

    SPI_EXECUTE_COMPAT(query.data, true, 0);

    if (SPI_processed > 0)
        elog(LOG, "Created DML trigger for delta table %u", delta_relid);
    else
        elog(WARNING, "Failed to create DML trigger for delta table %u", delta_relid);

    SPI_FINISH_COMPAT();
    pfree(query.data);
}

Oid
get_iceberg_table_oid(Oid delta_relid)
{
    Oid iceberg_relid = InvalidOid;

    SPI_CONNECT_COMPAT();

    StringInfoData query;
    initStringInfo(&query);

    appendStringInfo(&query,
        "SELECT iceberg_table_oid FROM gaussvector.delta_tables "
        "WHERE delta_table_oid = %u",
        delta_relid);

    SPI_EXECUTE_COMPAT(query.data, true, 0);

    if (SPI_processed > 0)
    {
        HeapTuple tuple = SPI_tuptable->vals[0];
        bool isnull;
        Datum datum = SPI_getbinval(tuple, SPI_tuptable->tupdesc, 1, &isnull);

        if (!isnull)
            iceberg_relid = DatumGetObjectId(datum);
    }

    SPI_FINISH_COMPAT();
    pfree(query.data);

    return iceberg_relid;
}

const char *
get_iceberg_location(Oid delta_relid)
{
    char *location = NULL;

    SPI_CONNECT_COMPAT();

    StringInfoData query;
    initStringInfo(&query);

    appendStringInfo(&query,
        "SELECT iceberg_location FROM gaussvector.delta_tables "
        "WHERE delta_table_oid = %u",
        delta_relid);

    SPI_EXECUTE_COMPAT(query.data, true, 0);

    if (SPI_processed > 0)
    {
        HeapTuple tuple = SPI_tuptable->vals[0];
        bool isnull;
        Datum datum = SPI_getbinval(tuple, SPI_tuptable->tupdesc, 1, &isnull);

        if (!isnull)
            location = TextDatumGetCString(datum);
    }

    SPI_FINISH_COMPAT();
    pfree(query.data);

    return location;
}

void
remove_delta_table_mapping(Oid delta_relid)
{
    SPI_CONNECT_COMPAT();

    StringInfoData query;
    initStringInfo(&query);

    /* 删除 DDL 日志 */
    appendStringInfo(&query,
        "DELETE FROM gaussvector.delta_ddl_log "
        "WHERE delta_table_oid = %u; ",
        delta_relid);

    /* 删除 DML 日志 */
    appendStringInfo(&query,
        "DELETE FROM gaussvector.delta_dml_log "
        "WHERE delta_table_oid = %u; ",
        delta_relid);

    /* 删除映射 */
    appendStringInfo(&query,
        "DELETE FROM gaussvector.delta_tables "
        "WHERE delta_table_oid = %u",
        delta_relid);

    SPI_EXECUTE_COMPAT(query.data, false, 0);
    SPI_FINISH_COMPAT();

    elog(LOG, "Removed delta table mapping and DDL logs for %u", delta_relid);
    pfree(query.data);
}

/* ==================== DDL 日志记录 ==================== */

/*
 * 记录 DDL 变化到 gaussvector.delta_ddl_log
 * FLUSH 时会读取此表批量同步到 Iceberg
 */
void
log_ddl_change(Oid delta_relid,
               const char *ddl_type,
               const char *column_name,
               const char *column_type,
               const char *new_type,
               bool is_not_null)
{
    SPI_CONNECT_COMPAT();

    StringInfoData query;
    initStringInfo(&query);

    /* 构建 JSONB 详情 */
    appendStringInfo(&query,
        "INSERT INTO gaussvector.delta_ddl_log "
        "(delta_table_oid, ddl_type, ddl_detail) "
        "VALUES (%u, '%s', "
        "jsonb_build_object("
        "'column_name', '%s', "
        "'column_type', '%s', "
        "'new_type', '%s', "
        "'is_not_null', %s))",
        delta_relid, ddl_type,
        column_name ? column_name : "",
        column_type ? column_type : "",
        new_type ? new_type : "",
        is_not_null ? "true" : "false");

    SPI_EXECUTE_COMPAT(query.data, false, 0);
    SPI_FINISH_COMPAT();

    elog(LOG, "Logged DDL change: %s on delta table %u, column %s",
         ddl_type, delta_relid, column_name ? column_name : "N/A");

    pfree(query.data);

    /* 更新 delta_tables 的 pending_changes 标记 */
    SPI_CONNECT_COMPAT();
    StringInfoData update_query;
    initStringInfo(&update_query);

    appendStringInfo(&update_query,
        "UPDATE gaussvector.delta_tables "
        "SET pending_changes = true "
        "WHERE delta_table_oid = %u",
        delta_relid);

    SPI_EXECUTE_COMPAT(update_query.data, false, 0);
    SPI_FINISH_COMPAT();

    pfree(update_query.data);
}

/* ==================== 辅助函数 ==================== */

const char *
get_namespace_name_safe(Oid namespace_oid)
{
    const char *name = get_namespace_name(namespace_oid);
    return name ? name : "public";
}

/* ==================== DML 日志记录 ==================== */

/*
 * log_dml_operation - 记录 UPDATE/DELETE 操作到 gaussvector.delta_dml_log
 *
 * 在 ExecutorRun hook 中调用，记录 DML 操作类型和原始 SQL。
 * pk_values 在 FLUSH 时由 PL/pgSQL 函数从 Delta 表解析填充。
 */
void
log_dml_operation(Oid delta_relid, const char *operation_type, const char *source_sql)
{
    SPI_CONNECT_COMPAT();

    StringInfoData query;
    initStringInfo(&query);

    /*
     * 记录 DML 操作到日志表。
     * pk_values 暂设为空 JSONB '{}'，FLUSH 时由 PL/pgSQL
     * 解析 source_sql 中的 WHERE 条件并从 Delta 表查询填充。
     */
    appendStringInfo(&query,
        "INSERT INTO gaussvector.delta_dml_log "
        "(delta_table_oid, operation_type, pk_values, source_sql) "
        "VALUES (%u, '%s', '{}'::jsonb, '%s')",
        delta_relid, operation_type,
        source_sql ? source_sql : "");

    SPI_EXECUTE_COMPAT(query.data, false, 0);
    SPI_FINISH_COMPAT();

    elog(LOG, "Logged DML operation: %s on delta table %u", operation_type, delta_relid);

    pfree(query.data);

    /* 同时标记 pending_changes */
    MarkDeltaTablePendingChanges(delta_relid);
}

/*
 * get_table_primary_keys - 获取表的主键列名列表
 *
 * 通过 SPI 查询 pg_constraint 获取主键约束列名。
 * 如果没有主键，返回所有列名作为标识。
 * 返回的数组通过 palloc 分配，需要调用者释放。
 */
char **
get_table_primary_keys(Oid delta_relid, int *pk_count)
{
    char **pk_columns = NULL;
    int count = 0;

    SPI_CONNECT_COMPAT();

    StringInfoData query;
    initStringInfo(&query);

    /* 查找主键约束列 */
    appendStringInfo(&query,
        "SELECT array_agg(a.attname ORDER BY k.n) "
        "FROM pg_constraint c "
        "CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS k(attnum, n) "
        "JOIN pg_attribute a ON a.attrelid = %u AND a.attnum = k.attnum "
        "WHERE c.conrelid = %u AND c.contype = 'p'",
        delta_relid, delta_relid);

    SPI_EXECUTE_COMPAT(query.data, true, 0);

    if (SPI_processed > 0)
    {
        HeapTuple tuple = SPI_tuptable->vals[0];
        bool isnull;
        Datum datum = SPI_getbinval(tuple, SPI_tuptable->tupdesc, 1, &isnull);

        if (!isnull)
        {
            /* 解析 ArrayType 到 char** 数组 */
            ArrayType *arr = DatumGetArrayTypeP(datum);
            Datum *elems;
            bool *nulls;
            int nelems;

            deconstruct_array(arr, TEXTOID, -1, false, 'i',
                              &elems, &nulls, &nelems);

            pk_columns = (char **) palloc(nelems * sizeof(char *));
            count = nelems;

            for (int i = 0; i < nelems; i++)
            {
                if (!nulls[i])
                    pk_columns[i] = TextDatumGetCString(elems[i]);
                else
                    pk_columns[i] = NULL;
            }
        }
    }

    /* 如果没有主键，使用所有列作为标识 */
    if (pk_columns == NULL || count == 0)
    {
        pfree(query.data);
        resetStringInfo(&query);

        appendStringInfo(&query,
            "SELECT array_agg(attname ORDER BY attnum) "
            "FROM pg_attribute "
            "WHERE attrelid = %u AND attnum > 0 AND NOT attisdropped",
            delta_relid);

        SPI_EXECUTE_COMPAT(query.data, true, 0);

        if (SPI_processed > 0)
        {
            HeapTuple tuple = SPI_tuptable->vals[0];
            bool isnull;
            Datum datum = SPI_getbinval(tuple, SPI_tuptable->tupdesc, 1, &isnull);

            if (!isnull)
            {
                ArrayType *arr = DatumGetArrayTypeP(datum);
                Datum *elems;
                bool *nulls;
                int nelems;

                deconstruct_array(arr, TEXTOID, -1, false, 'i',
                                  &elems, &nulls, &nelems);

                pk_columns = (char **) palloc(nelems * sizeof(char *));
                count = nelems;

                for (int i = 0; i < nelems; i++)
                {
                    if (!nulls[i])
                        pk_columns[i] = TextDatumGetCString(elems[i]);
                    else
                        pk_columns[i] = NULL;
                }
            }
        }
    }

    SPI_FINISH_COMPAT();
    pfree(query.data);

    *pk_count = count;
    return pk_columns;
}