/*
 * hook_executor.cpp - ExecutorRun Hook（延迟同步模式）
 *
 * 延迟同步模式下：
 * - INSERT 不立即同步到 Iceberg
 * - 数据保存在 Delta 内表中
 * - 用户调用 FLUSH 命令时批量同步
 *
 * 此 Hook 仅用于日志记录，不执行实际同步
 */

#include "../include/dual_table.h"

/*
 * ExecutorRun Hook：记录 INSERT 操作日志
 * 实际同步由 gaussvector.flush_delta_table() 执行
 */
void
delta_table_executor_run_hook(QueryDesc *queryDesc, ScanDirection direction, long count)
{
    /* 执行原始 ExecutorRun */
    if (ExecutorRun_hook && ExecutorRun_hook != delta_table_executor_run_hook)
        ExecutorRun_hook(queryDesc, direction, count);
    else
        standard_ExecutorRun(queryDesc, direction, count);

    /* INSERT 完成后，标记 Delta 表有待刷新的数据 */
    if (queryDesc->operation == CMD_INSERT)
    {
        ResultRelInfo *resultRelInfo = queryDesc->estate->es_result_relation_info;

        if (resultRelInfo && resultRelInfo->ri_RelationDesc)
        {
            Oid target_relid = resultRelInfo->ri_RelationDesc->rd_id;

            /* 检查是否是 Delta 表 */
            const char *location = get_iceberg_location(target_relid);
            if (location != NULL)
            {
                /* 更新 pending_changes 标记 */
                SPI_CONNECT_COMPAT();

                StringInfoData query;
                initStringInfo(&query);

                appendStringInfo(&query,
                    "UPDATE gaussvector.delta_tables "
                    "SET pending_changes = true "
                    "WHERE delta_table_oid = %u",
                    target_relid);

                SPI_EXECUTE_COMPAT(query.data, false, 0);
                SPI_FINISH_COMPAT();

                pfree(query.data);

                elog(DEBUG1, "Delta Table: INSERT on delta table %u, marked pending_changes",
                     target_relid);
            }
        }
    }
}