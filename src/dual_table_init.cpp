/*
 * dual_table_init.cpp - 扩展初始化（延迟同步模式）
 */

#include "../include/dual_table.h"

PG_MODULE_MAGIC;

/* Hook 保存指针 */
static ProcessUtility_hook_type prev_ProcessUtility_hook = NULL;
static ExecutorRun_hook_type prev_ExecutorRun_hook = NULL;

/* ==================== Hook 安装 ==================== */

void
InitializeDeltaTablePlugin(void)
{
    /* 保存原有 hook */
    prev_ProcessUtility_hook = ProcessUtility_hook;
    prev_ExecutorRun_hook = ExecutorRun_hook;

    /* 安装新 hook */
    ProcessUtility_hook = delta_table_process_utility_hook;
    ExecutorRun_hook = delta_table_executor_run_hook;

    elog(LOG, "Delta/Iceberg Plugin initialized (Delayed Sync Mode): hooks installed");
}

/* ==================== 扩展加载入口 ==================== */

extern "C" void
_PG_init(void)
{
    InitializeDeltaTablePlugin();
}