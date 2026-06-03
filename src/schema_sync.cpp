/*
 * schema_sync.cpp - 辅助函数（名称生成和 SQL 构建）
 *
 * 注意：延迟同步模式下，DDL 不立即同步
 * 实际同步由 gaussvector.flush_delta_table() 函数执行
 */

#include "../include/dual_table.h"

/* ==================== 名称生成 ==================== */

char *
generate_iceberg_table_name(const char *original_name)
{
    return psprintf("%s%s", original_name, ICEBERG_SUFFIX);
}

char *
generate_iceberg_location(const char *schema_name, const char *table_name)
{
    return psprintf("%s/%s/%s", ICEBERG_WAREHOUSE, schema_name, table_name);
}

/* ==================== SQL 构建 ==================== */

/*
 * 构建 CREATE FOREIGN TABLE SQL
 * 用于 FLUSH 时首次创建 Iceberg 外表
 */
char *
build_create_foreign_table_sql(CreateStmt *stmt,
                                Oid namespace_oid,
                                const char *iceberg_name,
                                const char *location)
{
    StringInfoData sql;
    initStringInfo(&sql);

    const char *namespace_name = get_namespace_name_safe(namespace_oid);

    appendStringInfo(&sql, "CREATE FOREIGN TABLE %s.%s (",
                     namespace_name, iceberg_name);

    /* 复制所有列定义 */
    ListCell *cell;
    bool first = true;

    foreach(cell, stmt->tableElts)
    {
        Node *element = (Node *) lfirst(cell);

        if (IsA(element, ColumnDef))
        {
            ColumnDef *colDef = (ColumnDef *) element;

            if (!first)
                appendStringInfo(&sql, ", ");

            /* 列名 */
            appendStringInfo(&sql, "%s ", quote_identifier(colDef->colname));

            /* 列类型 */
            if (colDef->typname)
            {
                Oid typoid;
                int32 typmod = 0;
                typenameTypeIdAndMod(NULL, colDef->typname, &typoid, &typmod, NULL);
                appendStringInfo(&sql, "%s", format_type_with_typemod(typoid, typmod));
            }

            /* NOT NULL */
            if (colDef->is_not_null)
                appendStringInfo(&sql, " NOT NULL");

            first = false;
        }
    }

    appendStringInfo(&sql, ") SERVER iceberg_server ");
    appendStringInfo(&sql, "OPTIONS (location '%s', table_name '%s')",
                     location, stmt->relation->relname);

    return sql.data;
}