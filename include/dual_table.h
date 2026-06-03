/*
 * dual_table.h - Delta/Iceberg 双表架构核心头文件（延迟同步模式）
 *
 * 架构说明：
 * - Delta 表（ustore 内表）：接收所有 INSERT 和 DDL 操作
 * - Iceberg 表（外表）：存储最终数据（Parquet 格式）
 * - 延迟同步：DDL 和 INSERT 不立即同步，记录到日志表
 * - FLUSH 命令：用户主动触发批量同步
 */

#ifndef DUAL_TABLE_H
#define DUAL_TABLE_H

/* openGauss 核心头文件 - 必须最先包含 */
#include "postgres.h"
#include "knl/knl_variable.h"

/* 安全函数 */
#include "securec.h"
#include "securec_check.h"

/* PostgreSQL 基础 */
#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"

/* Catalog */
#include "catalog/pg_type.h"
#include "catalog/pg_class.h"
#include "catalog/pg_namespace.h"

/* Access */
#include "access/htup.h"
#include "access/xact.h"

/* Executor */
#include "executor/executor.h"
#include "executor/tuptable.h"
#include "executor/exec/execdesc.h"
#include "executor/spi.h"

/* Nodes */
#include "nodes/pg_list.h"
#include "nodes/makefuncs.h"
#include "nodes/value.h"
#include "nodes/relation.h"
#include "nodes/execnodes.h"
#include "nodes/plannodes.h"
#include "nodes/parsenodes.h"

/* Parser */
#include "parser/parsetree.h"
#include "parser/parse_type.h"

/* Commands */
#include "commands/tablecmds.h"
#include "commands/defrem.h"

/* TCOP */
#include "tcop/utility.h"

/* Misc */
#include "miscadmin.h"

/* Foreign */
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"

/* 兼容层 */
#include "opengauss_compat.h"

/* ==================== 常量定义 ==================== */

#define DELTA_TABLE_VERSION "Delta/Iceberg Plugin v1.0 (Delayed Sync Mode)"

/* 外表命名规则：delta表名_iceberg */
#define ICEBERG_SUFFIX "_iceberg"

/* 默认 Iceberg 存储根路径 */
#define ICEBERG_WAREHOUSE "/data/iceberg"

/* ==================== Catalog 表结构 ==================== */

/*
 * gaussvector.delta_tables - Delta/Iceberg 映射 catalog
 */
typedef struct DeltaTableMapping
{
    Oid         delta_table_oid;        /* Delta 内表 OID */
    Oid         iceberg_table_oid;      /* Iceberg 外表 OID（首次 FLUSH 后创建） */
    char       *iceberg_location;       /* Iceberg 存储路径 */
    bool        pending_changes;        /* 是否有待刷新的数据 */
    int         schema_version;         /* Schema 版本号 */
    TimestampTz delta_created_at;       /* Delta 表创建时间 */
    TimestampTz last_flush_at;          /* 最后 FLUSH 时间 */
} DeltaTableMapping;

/* ==================== Hook 函数声明 ==================== */

extern void delta_table_process_utility_hook(
    processutility_context *cxt,
    DestReceiver *dest,
    bool sentToRemote,
    char *completionTag,
    ProcessUtilityContext context,
    bool isCTAS);

extern void delta_table_executor_run_hook(
    QueryDesc *queryDesc,
    ScanDirection direction,
    long count);

/* ==================== Catalog 操作函数 ==================== */

/* 注册 Delta 表映射 */
extern void register_delta_table_mapping(
    Oid delta_relid,
    const char *location);

/* 获取 Iceberg 外表 OID（可能为 NULL，直到首次 FLUSH） */
extern Oid get_iceberg_table_oid(Oid delta_relid);

/* 删除 Delta 表映射 */
extern void remove_delta_table_mapping(Oid delta_relid);

/* 获取 Iceberg 存储路径 */
extern const char *get_iceberg_location(Oid delta_relid);

/* 辅助函数 */
extern const char *get_namespace_name_safe(Oid namespace_oid);

/* ==================== DDL 日志记录函数 ==================== */

/* 记录 DDL 变化到日志表（延迟同步） */
extern void log_ddl_change(
    Oid delta_relid,
    const char *ddl_type,
    const char *column_name,
    const char *column_type,
    const char *new_type,
    bool is_not_null);

/* ==================== Schema 同步函数 ==================== */

/* 生成 Iceberg 外表名称 */
extern char *generate_iceberg_table_name(const char *original_name);

/* 生成 Iceberg 存储路径 */
extern char *generate_iceberg_location(
    const char *schema_name,
    const char *table_name);

/* 构建 CREATE FOREIGN TABLE SQL */
extern char *build_create_foreign_table_sql(
    CreateStmt *stmt,
    Oid namespace_oid,
    const char *iceberg_name,
    const char *location);

/* ==================== 初始化函数 ==================== */

extern void InitializeDeltaTablePlugin(void);

#endif /* DUAL_TABLE_H */