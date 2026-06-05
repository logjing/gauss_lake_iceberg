/*
 * dual_table_init.cpp - 扩展初始化（延迟同步模式）
 *
 * 参考 security_plugin 的模式：
 * - 只在 WORKER/THREADPOOL_WORKER 线程中安装 hook
 * - 使用 THR_LOCAL 保存 prev hook（同编译单元内使用）
 * - 检查 process_shared_preload_libraries_in_progress
 */

#include "../include/dual_table.h"

/* CRITICAL: Override PG_VERSION_NUM AFTER includes, BEFORE PG_MODULE_MAGIC */
#undef PG_VERSION_NUM
#define PG_VERSION_NUM 90204

PG_MODULE_MAGIC;

/* Hook 保存指针 - THR_LOCAL matches kernel hook storage type.
 * MUST be in same compilation unit as hook functions that use them,
 * since THR_LOCAL (__thread) variables cannot be extern'd across .so units. */
static THR_LOCAL ProcessUtility_hook_type prev_ProcessUtility_hook = NULL;
static THR_LOCAL ExecutorRun_hook_type prev_ExecutorRun_hook = NULL;

/* ==================== Helper: call previous hook chain ==================== */

/*
 * These wrapper functions allow other .cpp files to call the prev hooks
 * without needing direct access to THR_LOCAL variables.
 */

void call_prev_ProcessUtility(processutility_context *cxt, DestReceiver *dest,
                               bool sentToRemote, char *completionTag,
                               ProcessUtilityContext context, bool isCTAS)
{
    if (prev_ProcessUtility_hook)
        prev_ProcessUtility_hook(cxt, dest, sentToRemote, completionTag, context, isCTAS);
    else
        standard_ProcessUtility(cxt, dest, sentToRemote, completionTag, context, isCTAS);
}

void call_prev_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, long count)
{
    if (prev_ExecutorRun_hook)
        prev_ExecutorRun_hook(queryDesc, direction, count);
    else
        standard_ExecutorRun(queryDesc, direction, count);
}

/* ==================== Hook 安装 ==================== */

void
InitializeDeltaTablePlugin(void)
{
    /* Save existing hooks (thread-local) - preserves hook chain */
    prev_ProcessUtility_hook = ProcessUtility_hook;
    prev_ExecutorRun_hook = ExecutorRun_hook;

    /* Install our hooks (thread-local) */
    ProcessUtility_hook = delta_table_process_utility_hook;
    ExecutorRun_hook = delta_table_executor_run_hook;

    elog(LOG, "Delta/Iceberg Plugin initialized (Delayed Sync Mode): hooks installed, "
             "ER_hook addr=0x%lx",
         (unsigned long)ExecutorRun_hook);
}

/* ==================== 扩展加载入口 ==================== */

extern "C" void
_PG_init(void)
{
    /* Following security_plugin's pattern:
     * Only install hooks in WORKER/THREADPOOL_WORKER threads.
     * _PG_init is called in postmaster (skip) and in each worker thread (install). */
    if (t_thrd.role != WORKER && t_thrd.role != THREADPOOL_WORKER)
    {
        return;
    }

    InitializeDeltaTablePlugin();
}