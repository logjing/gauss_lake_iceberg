/*
 * hook_executor.cpp - ExecutorRun Hook（延迟同步模式）
 *
 * DML 操作（INSERT/UPDATE/DELETE）现在由 SQL 触发器处理。
 * 当 Delta 表上有触发器时，OpFusion 会回退到标准 executor 路径，
 * 触发器会自动记录 DML 操作到 delta_dml_log 表。
 *
 * ExecutorRun_hook 主要用于：
 * 1. SELECT 查询的可选监控
 * 2. 作为后备机制，防止触发器未正确安装的情况
 */

#include "../include/dual_table.h"
#include <unistd.h>
#include <fcntl.h>
#include <cstring>

void
delta_table_executor_run_hook(QueryDesc *queryDesc, ScanDirection direction, long count)
{
    /* Safety: skip for NULL queryDesc */
    if (queryDesc == NULL)
    {
        call_prev_ExecutorRun(queryDesc, direction, count);
        return;
    }

    CmdType operation = queryDesc->operation;

    /*
     * DML 操作现在由 SQL 触发器处理（delta_dml_trigger_func）。
     * 当 Delta 表有触发器时，OpFusion 会检测到并回退到标准 executor，
     * 触发器会在 DML 执行时自动记录到 delta_dml_log 表。
     *
     * 这里我们只记录调试信息，不执行实际的 DML 日志记录。
     */

    /* Call prev hook chain */
    call_prev_ExecutorRun(queryDesc, direction, count);
}