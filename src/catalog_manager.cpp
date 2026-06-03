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
 */
void
register_delta_table_mapping(Oid delta_relid, const char *location)
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
    SPI_FINISH_COMPAT();

    elog(LOG, "Registered delta table %u with location %s", delta_relid, location);
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