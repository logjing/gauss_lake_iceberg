# Delta/Iceberg 双表架构需求文档

## 需求总览

| 编号 | 需求名称 | 阶段 | 优先级 | 描述 |
|:---:|---------|:---:|:-----:|------|
| 1 | DDL 级联处理 Delta 表 | 写入层 | P0 | CREATE/DROP/ALTER 自动级联到 Iceberg 外表 |
| 2 | COPY FROM 通过 Delta 层攒批 | 写入层 | P0 | COPY FROM 数据先写入 Delta 表，不直接写 Iceberg |
| 3 | INSERT 通过 Delta 单条攒批 | 写入层 | P0 | INSERT 数据先写入 Delta 表，标记待刷新 |
| 4 | Tuple-Arrow 格式双向转换 | 转换层 | P0 | gaussvector 支持 Tuple 和 Arrow 格式互转 |
| 5 | GV 写 Parquet + 更新元信息 | 写入层 | P0 | 将数据写入 Parquet 文件，更新 Iceberg 元信息 |
| 6 | UD 级联处理 Delta 层数据 | 写入层 | P0 | UPDATE/DELETE 记录到 DML 日志，FLUSH 时同步 |
| 7 | UD 回表 + 标记删除 + 插入 Delta | 写入层 | P1 | 条件不在 Delta 时，从 Iceberg 回表处理 |
| 8 | 有锁批量刷新到 Iceberg | 刷新层 | P0 | 系统函数 `flush_delta_table()` 有锁刷新 |
| 9 | 后台自动刷新（单节点） | 刷新层 | P1 | 后台 worker 定期自动刷新 Delta 数据 |
| 10 | 故障后自动找执行节点 | 刷新层 | P2 | 刷新失败后自动重试和故障转移 |
| 11 | 标记删除的批量刷新 | 刷新层 | P1 | 支持带有删除标记的数据刷新到 Iceberg |
| 12 | 指定数据量的微批 Flush | 刷新层 | P1 | 分批刷新，避免一次性大量数据写入 |
| 13 | 行级语义 Flush 原子性 | 刷新层 | P1 | 同一行的 UPDATE 不能仅刷新 DELETE |
| 14 | Flush 后索引维护 | 刷新层 | P2 | 刷新后自动更新统计信息和索引 |
| 15 | Iceberg V3 Spec 支持 | 存储层 | P0 | 支持 COW/MOR 两种模式，符合 V3 规范 |

**阶段说明**：
- **写入层**：数据写入 Delta 表的处理逻辑
- **转换层**：数据格式转换（Tuple ↔ Arrow）
- **刷新层**：Delta 数据同步到 Iceberg 的处理逻辑
- **存储层**：Iceberg 存储格式和规范支持

**优先级说明**：
- **P0**：核心功能，必须实现
- **P1**：重要功能，优先实现
- **P2**：增强功能，后续实现

**测试状态**：

| 编号 | 需求名称 | 实现状态 | 测试状态 | 备注 |
|:---:|---------|:-------:|:-------:|------|
| 1 | DDL 级联处理 | ⚠️ 部分实现 | 🔲 待测试 | hook_process_utility.cpp 仅调用 prev hook，未实现 DDL 级联逻辑 |
| 2 | COPY FROM 攒批 | ✅ 已实现 | 🔲 待测试 | delta_copy_from.cpp 已实现，需验证触发器兼容性 |
| 3 | INSERT 攒批 | ✅ 已实现 | ✅ 已测试 | 触发器 delta_dml_trigger_func 正常工作 |
| 4 | Tuple-Arrow 转换 | ✅ 已实现 | 🔲 待测试 | arrow_conversion.cpp 已实现，Parquet I/O 为 stub |
| 5 | GV 写 Parquet | ⚠️ 部分实现 | 🔲 待测试 | 接口已定义，Parquet 写入为 stub (TODO) |
| 6 | UD 级联 | ✅ 已实现 | ✅ 已测试 | test_mor_ud.sql 测试通过 |
| 7 | UD 回表 | ✅ 已实现 | ✅ 已测试 | test_back_to_table_v2.sql 测试通过 |
| 8 | 有锁批量刷新 | ✅ 已实现 | 🔲 待测试 | flush_delta_table() 函数已实现 |
| 9 | 后台自动刷新 | 🔲 待实现 | 🔲 待测试 | 需要 background worker |
| 10 | 故障自动恢复 | 🔲 待实现 | 🔲 待测试 | 需要重试机制 |
| 11 | 标记删除刷新 | ✅ 已实现 | 🔲 待测试 | delta_iceberg_delete_log 已实现 |
| 12 | 微批 Flush | 🔲 待实现 | 🔲 待测试 | 需要分批逻辑 |
| 13 | 行级原子性 | 🔲 待实现 | 🔲 待测试 | 需要事务分组 |
| 14 | 索引维护 | 🔲 待实现 | 🔲 待测试 | 需要 ANALYZE/REINDEX |
| 15 | Iceberg V3 | 🔲 待实现 | 🔲 待测试 | 需要 COW/MOR 模式选择 |

---

## 项目概述

基于 openGauss 的 Delta/Iceberg 双表架构插件，实现数据的延迟同步机制。Delta 表（ustore 内表）接收所有写入操作，Iceberg 表（外表）存储最终数据。通过 FLUSH 操作将 Delta 层数据批量同步到 Iceberg。

## 架构设计

```
┌─────────────────────────────────────────────────────────────────┐
│                        SQL 操作层                               │
│  INSERT / UPDATE / DELETE / COPY FROM / DDL (CREATE/ALTER/DROP) │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Delta 表 (ustore)                          │
│  - 接收所有写入操作                                              │
│  - 记录 DDL/DML 变化到日志表                                     │
│  - 支持单条数据攒批                                              │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                    FLUSH 操作 (手动/自动)                        │
│  - 批量同步数据到 Iceberg                                        │
│  - 支持微批、有锁/无锁模式                                       │
│  - 行级语义原子性保证                                            │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Iceberg 表 (Parquet)                         │
│  - 存储最终数据                                                  │
│  - 更新 Iceberg 元信息                                           │
│  - 支持 COW/MOR 模式                                            │
└─────────────────────────────────────────────────────────────────┘
```

---

## 需求列表

### 1. DDL 级联处理 Delta 表

**需求描述**：支持对 Delta 表执行 DDL 操作（CREATE/DROP/ALTER），自动级联到 Iceberg 外表。

**功能点**：

| 操作 | 描述 | 实现方式 |
|------|------|---------|
| CREATE TABLE | 创建 Delta 表时自动注册映射关系，创建 DML 触发器 | ProcessUtility hook |
| DROP TABLE | 删除 Delta 表时自动删除 Iceberg 外表和元数据 | ProcessUtility hook |
| ALTER TABLE | 修改 Delta 表结构时记录 DDL 日志，FLUSH 时同步到 Iceberg | ProcessUtility hook |

**DDL 变化类型**：
- `ADD_COLUMN`：新增列
- `DROP_COLUMN`：删除列
- `ALTER_TYPE`：修改列类型
- `RENAME_COLUMN`：重命名列
- `SET_NOT_NULL`：设置非空约束

**日志表结构**：
```sql
CREATE TABLE gaussvector.delta_ddl_log (
    id serial PRIMARY KEY,
    delta_table_oid regclass NOT NULL,
    ddl_type text NOT NULL,
    ddl_detail jsonb NOT NULL,
    created_at timestamp DEFAULT current_timestamp,
    flushed boolean DEFAULT false
);
```

**验收标准**：
- [ ] CREATE TABLE 后自动注册到 `delta_tables`
- [ ] DROP TABLE 后自动清理 Iceberg 外表和日志
- [ ] ALTER TABLE 变化记录到 `delta_ddl_log`
- [ ] FLUSH 时 DDL 变化同步到 Iceberg 外表

---

### 2. 支持 COPY FROM 通过 Delta 层单条数据攒批

**需求描述**：COPY FROM 命令导入的数据先写入 Delta 表，不直接写入 Iceberg，支持单条数据攒批。

**功能点**：
- 拦截 COPY FROM 操作
- 数据逐行写入 Delta 表
- 标记 `pending_changes = true`
- 不立即同步到 Iceberg

**实现方式**：
```
COPY FROM → ExecutorRun_hook → 写入 Delta 表 → 标记待刷新
```

**验收标准**：
- [ ] COPY FROM 数据正确写入 Delta 表
- [ ] 不直接写入 Iceberg 表
- [ ] `pending_changes` 标记正确
- [ ] 支持大批量数据导入（>100万行）

---

### 3. 支持 INSERT 通过 Delta 单条数据攒批

**需求描述**：INSERT 操作的数据先写入 Delta 表，支持单条数据攒批，不立即同步到 Iceberg。

**功能点**：
- INSERT 数据写入 Delta 表
- 触发器记录 INSERT 操作
- 标记 `pending_changes = true`

**触发器设计**：
```sql
-- AFTER INSERT 触发器
CREATE TRIGGER delta_dml_trigger_xxx
AFTER INSERT ON delta_table
FOR EACH ROW
EXECUTE FUNCTION gaussvector.delta_dml_trigger_func();
```

**验收标准**：
- [ ] INSERT 数据正确写入 Delta 表
- [ ] 触发器正确记录操作
- [ ] `pending_changes` 标记正确
- [ ] 支持批量 INSERT

---

### 4. gaussvector 支持 Tuple-Arrow 格式双向转换

**需求描述**：gaussvector (GV) 支持 openGauss 内部 Tuple 格式与 Apache Arrow 格式之间的双向转换，为 Parquet 写入提供数据格式基础。

**功能点**：

| 转换方向 | 描述 | 应用场景 |
|---------|------|---------|
| Tuple → Arrow | 将 openGauss Tuple 数据转换为 Arrow 格式 | FLUSH 时从 Delta 表读取数据，转换后写入 Parquet |
| Arrow → Tuple | 将 Arrow 格式数据转换为 openGauss Tuple | 从 Parquet/Iceberg 读取数据，转换后插入 Delta 表 |

**技术架构**：
```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  openGauss      │     │   gaussvector   │     │  Apache Arrow   │
│  Tuple 格式     │ ──→ │   转换引擎      │ ──→ │  列式格式       │
│  (行式存储)     │ ←── │   (Tuple↔Arrow) │ ←── │  (列式存储)     │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

**Tuple 格式（openGauss 内部）**：
```c
typedef struct HeapTupleData {
    uint32 t_len;           /* 元组长度 */
    ItemPointerData t_self; /* 自引用 TID */
    Oid t_tableOid;         /* 所属表 OID */
    HeapTupleHeader t_data; /* 元组头 + 数据 */
} HeapTupleData;
```

**Arrow 格式（列式存储）**：
```
Arrow RecordBatch:
├── Schema (列定义)
├── Column 0: [val1, val2, val3, ...] (Arrow Array)
├── Column 1: [val1, val2, val3, ...] (Arrow Array)
└── ...
```

**转换函数接口**：
```c
/* Tuple → Arrow */
ArrowRecordBatch *tuple_to_arrow(
    TupleDesc tupdesc,
    HeapTuple *tuples,
    int num_tuples
);

/* Arrow → Tuple */
HeapTuple *arrow_to_tuple(
    TupleDesc tupdesc,
    ArrowRecordBatch *batch,
    int *num_tuples
);
```

**依赖库**：
- Apache Arrow C++ 库 (`libarrow`)
- Apache Arrow GLib（可选）

**验收标准**：
- [ ] Tuple → Arrow 转换正确（数据类型映射）
- [ ] Arrow → Tuple 转换正确（数据类型映射）
- [ ] 支持所有 openGauss 数据类型
- [ ] 性能满足 FLUSH 吞吐量要求（>50,000 行/秒）
- [ ] 内存使用合理，支持大数据量转换

---

### 5. GV 支持写 Parquet 文件、更新 Iceberg 元信息

**需求描述**：FLUSH 操作时，gaussvector (GV) 支持将数据写入 Parquet 文件，并更新 Iceberg 元信息。

**功能点**：
- 将 Delta 表数据导出为 Parquet 格式
- 写入到 Iceberg 存储路径
- 更新 Iceberg 元数据文件（metadata.json）
- 更新快照信息（snap-xxx.avro）

**Iceberg 元数据结构**：
```
iceberg_location/
├── data/
│   └── 00000-0-xxx.parquet
├── metadata/
│   ├── v1.metadata.json
│   ├── v2.metadata.json
│   └── snap-123456.avro
└── metadata.json (最新版本指针)
```

**验收标准**：
- [ ] Parquet 文件格式正确
- [ ] Iceberg 元数据文件完整
- [ ] 快照信息正确
- [ ] 支持 Iceberg 读取工具（如 Spark、Trino）读取

---

### 6. 支持 UD 级联处理 Delta 层已存在数据

**需求描述**：UPDATE/DELETE 操作级联处理 Delta 层已存在的数据，记录到 DML 日志，FLUSH 时同步到 Iceberg。

**功能点**：
- UPDATE 操作记录旧行主键值和新值
- DELETE 操作记录旧行主键值
- DML 日志存储在 `delta_dml_log` 表
- FLUSH 时根据日志同步到 Iceberg

**DML 日志表结构**：
```sql
CREATE TABLE gaussvector.delta_dml_log (
    id serial PRIMARY KEY,
    delta_table_oid regclass NOT NULL,
    operation_type text NOT NULL,  -- 'UPDATE', 'DELETE'
    pk_values jsonb NOT NULL,
    new_values jsonb,
    source_sql text,
    created_at timestamp DEFAULT current_timestamp,
    flushed boolean DEFAULT false
);
```

**FLUSH 同步策略**：
| 操作 | MOR 模式 | COW 模式 |
|------|----------|----------|
| UPDATE | DELETE 旧行 + INSERT 新行 | 全表重建 |
| DELETE | 标记删除 | 全表重建 |

**验收标准**：
- [ ] UPDATE 操作正确记录到 `delta_dml_log`
- [ ] DELETE 操作正确记录到 `delta_dml_log`
- [ ] FLUSH 时 DML 变化同步到 Iceberg
- [ ] 支持 MOR 和 COW 两种模式

---

### 7. 支持 UD 条件不在 Delta 时回表、标记删除、插入 Delta 表

**需求描述**：当 UPDATE/DELETE 的 WHERE 条件匹配的行不在 Delta 表中，而在 Iceberg 表中时，自动回表处理。

**功能点**：
- BEFORE STATEMENT 触发器检查是否需要回表
- 从 Iceberg 表读取匹配的行数据
- 标记 Iceberg 行已删除（记录到 `delta_iceberg_delete_log`）
- 将行数据插入 Delta 表
- 第二次 UPDATE/DELETE 成功修改数据

**流程图**：
```
UPDATE/DELETE delta_table WHERE condition
         │
         ▼
BEFORE STATEMENT 触发器
         │
         ├── 检查 WHERE 条件匹配的行是否在 Delta 表中
         │
         ▼ [不在 Delta 中]
从 Iceberg 表读取行数据
         │
         ├── 标记 Iceberg 行已删除
         ├── 插入行数据到 Delta 表
         └── 返回（本次 DML 返回 0 行）
         │
         ▼ [第二次执行]
DML 操作成功（行已在 Delta 中）
```

**日志表结构**：
```sql
CREATE TABLE gaussvector.delta_iceberg_delete_log (
    id serial PRIMARY KEY,
    delta_table_oid regclass NOT NULL,
    iceberg_table_oid regclass NOT NULL,
    pk_values jsonb NOT NULL,
    row_data jsonb,
    created_at timestamp DEFAULT current_timestamp,
    flushed boolean DEFAULT false
);
```

**验收标准**：
- [ ] 回表机制正确触发
- [ ] Iceberg 行正确标记删除
- [ ] Delta 表正确插入回表数据
- [ ] 第二次 DML 操作成功
- [ ] DML 日志正确记录

---

### 8. 支持 Delta 层新增数据通过系统函数有锁批量刷新到 Iceberg

**需求描述**：提供系统函数 `flush_delta_table()`，支持有锁模式批量刷新 Delta 层数据到 Iceberg。

**功能点**：
- 获取 Delta 表的排他锁
- 批量读取 Delta 表数据
- 写入 Parquet 文件
- 更新 Iceberg 元信息
- 清理已同步的 Delta 数据（可选）
- 释放锁

**函数签名**：
```sql
CREATE OR REPLACE FUNCTION gaussvector.flush_delta_table(
    delta_table regclass,
    cleanup boolean DEFAULT true
) RETURNS jsonb;
```

**返回值**：
```json
{
    "rows_synced": 1000,
    "ddl_synced": 2,
    "dml_synced": 5,
    "sync_mode": "COW",
    "duration_ms": 1234
}
```

**Debug 版本支持**：
- 支持刷到 CSV 文件（用于调试）
- 详细日志输出

**验收标准**：
- [ ] 有锁模式正确获取和释放锁
- [ ] 数据正确同步到 Iceberg
- [ ] 返回值信息完整
- [ ] Debug 版本支持 CSV 输出

---

### 9. 支持 Delta 层数据后台自动刷新（单节点）

**需求描述**：支持后台自动刷新 Delta 层数据到 Iceberg，单节点模式。

**功能点**：
- 后台 worker 定期检查 `pending_changes` 标记
- 自动触发 FLUSH 操作
- 可配置刷新间隔
- 单节点模式（不支持分布式）

**配置参数**：
```sql
-- 设置自动刷新间隔（秒）
ALTER SYSTEM SET gaussvector.auto_flush_interval = 60;

-- 启用/禁用自动刷新
ALTER SYSTEM SET gaussvector.auto_flush_enabled = true;

-- 设置批量大小
ALTER SYSTEM SET gaussvector.auto_flush_batch_size = 10000;
```

**实现方式**：
- 使用 openGauss background worker
- 定期扫描 `delta_tables` 表
- 检查 `pending_changes = true` 的记录
- 调用 `flush_delta_table()` 函数

**验收标准**：
- [ ] 后台 worker 正确启动
- [ ] 定时检查并刷新
- [ ] 配置参数生效
- [ ] 单节点模式正常工作

---

### 10. 支持 Delta 层数据故障后自动找执行节点

**需求描述**：当 Delta 层数据刷新失败时，支持自动重试并找到可用的执行节点。

**功能点**：
- 记录刷新失败的日志
- 自动重试机制
- 节点健康检查
- 故障转移

**日志表结构**：
```sql
CREATE TABLE gaussvector.flush_error_log (
    id serial PRIMARY KEY,
    delta_table_oid regclass NOT NULL,
    error_message text,
    retry_count int DEFAULT 0,
    next_retry_at timestamp,
    created_at timestamp DEFAULT current_timestamp,
    resolved boolean DEFAULT false
);
```

**重试策略**：
- 指数退避：1s, 2s, 4s, 8s, ...
- 最大重试次数：5 次
- 超过重试次数后标记为失败

**验收标准**：
- [ ] 失败日志正确记录
- [ ] 自动重试机制工作
- [ ] 节点故障检测
- [ ] 故障转移成功

---

### 11. 支持带有标记删除的 Delta 层批量刷新到 Iceberg

**需求描述**：支持将带有标记删除记录的 Delta 层数据批量刷新到 Iceberg。

**功能点**：
- 读取 `delta_iceberg_delete_log` 表
- 根据标记删除记录，从 Iceberg 中删除对应行
- 支持 MOR 和 COW 两种模式

**MOR 模式**：
```sql
-- 从 Iceberg 表删除标记的行
DELETE FROM iceberg_table WHERE pk = ...;
```

**COW 模式**：
```sql
-- 全表重建，排除标记删除的行
SELECT * FROM iceberg_table
WHERE pk NOT IN (SELECT pk_values FROM delta_iceberg_delete_log);
```

**验收标准**：
- [ ] 标记删除记录正确读取
- [ ] MOR 模式删除正确
- [ ] COW 模式重建正确
- [ ] 删除日志标记为已刷新

---

### 12. 支持指定数据量的微批 Flush

**需求描述**：支持指定数据量的微批 Flush，避免一次性刷新大量数据导致的性能问题。

**功能点**：
- 指定每批刷新的数据量
- 分批读取 Delta 表数据
- 分批写入 Iceberg
- 支持断点续传

**函数签名**：
```sql
CREATE OR REPLACE FUNCTION gaussvector.flush_delta_table_micro_batch(
    delta_table regclass,
    batch_size int DEFAULT 10000
) RETURNS jsonb;
```

**实现逻辑**：
```sql
-- 分批读取
SELECT * FROM delta_table
WHERE ctid > last_processed_ctid
ORDER BY ctid
LIMIT batch_size;

-- 写入 Iceberg
-- 更新 last_processed_ctid
```

**验收标准**：
- [ ] 指定批量大小生效
- [ ] 分批正确执行
- [ ] 断点续传支持
- [ ] 性能优于全量刷新

---

### 13. 支持行级语义 Flush 原子性保证

**需求描述**：保证同一行的 UPDATE 操作在 Flush 时的原子性，不能仅刷新 DELETE 而不刷新 INSERT。

**问题场景**：
```
UPDATE test SET name = 'new' WHERE id = 1;
-- DML 日志记录：
-- 1. DELETE id=1 (旧行)
-- 2. INSERT id=1 (新行)
-- Flush 时必须保证两行同时刷新，否则会导致数据丢失
```

**解决方案**：
- 使用事务保证原子性
- 同一 DML 操作的日志记录绑定在一起
- Flush 时按事务分组处理

**实现方式**：
```sql
-- DML 日志增加事务 ID
ALTER TABLE gaussvector.delta_dml_log
ADD COLUMN xid xid DEFAULT txid_current();

-- Flush 时按事务分组
SELECT * FROM gaussvector.delta_dml_log
WHERE xid = target_xid
ORDER BY id;
```

**验收标准**：
- [ ] 同一 UPDATE 的 DELETE/INSERT 同时刷新
- [ ] 事务中断时回滚
- [ ] 不会出现部分刷新
- [ ] 数据一致性保证

---

### 14. 支持 Flush 后索引维护

**需求描述**：Flush 操作后，自动维护 Iceberg 表的索引（如果支持）。

**功能点**：
- 更新 Iceberg 表的统计信息
- 重建 Iceberg 表的索引（如果 FDW 支持）
- 更新 `pg_statistic` 统计信息

**实现方式**：
```sql
-- Flush 后执行
ANALYZE iceberg_table;

-- 如果支持索引
REINDEX TABLE iceberg_table;
```

**验收标准**：
- [ ] 统计信息正确更新
- [ ] 索引重建成功（如果支持）
- [ ] 查询性能不下降

---

### 15. 支持写入符合 Iceberg V3 Spec 的数据和元数据

**需求描述**：支持写入符合 Apache Iceberg V3 规范的数据和元数据，支持 COW 和 MOR 两种模式二选一。

**Iceberg V3 Spec 要求**：
- 数据文件：Parquet 格式
- 元数据文件：JSON + Avro
- 快照管理：支持多版本
- 分区支持：支持分区表
- 行级删除：支持 Position Delete 和 Equality Delete

**COW 模式（Copy-On-Write）**：
- 每次 Flush 重写整个数据文件
- 适合小表、低频更新场景
- 实现简单，性能稳定

**MOR 模式（Merge-On-Read）**：
- 增量写入删除文件
- 适合大表、高频更新场景
- 读取时合并，写入性能好

**模式选择**：
```sql
-- 创建表时指定模式
CREATE TABLE test (id int PRIMARY KEY, name text)
WITH (iceberg_mode = 'COW');  -- 或 'MOR'

-- 修改模式
ALTER TABLE test SET (iceberg_mode = 'MOR');
```

**验收标准**：
- [ ] Parquet 文件符合规范
- [ ] 元数据文件完整
- [ ] COW 模式正确实现
- [ ] MOR 模式正确实现
- [ ] 支持 Iceberg 读取工具（Spark、Trino）读取
- [ ] 支持分区表
- [ ] 支持 Position Delete 和 Equality Delete

---

## 测试用例

### 单元测试

| 测试项 | 测试文件 | 状态 |
|--------|----------|------|
| DDL 级联 | `test_ddl_cascade.sql` | 待实现 |
| COPY FROM 攒批 | `test_copy_from.sql` | 待实现 |
| INSERT 攒批 | `test_insert_batch.sql` | 待实现 |
| UD 级联 | `test_mor_ud.sql` | ✅ 通过 |
| 回表机制 | `test_back_to_table.sql` | ✅ 通过 |
| 有锁 Flush | `test_flush_locked.sql` | 待实现 |
| 自动刷新 | `test_auto_flush.sql` | 待实现 |
| 微批 Flush | `test_micro_batch.sql` | 待实现 |
| 原子性保证 | `test_atomicity.sql` | 待实现 |
| Iceberg V3 | `test_iceberg_v3.sql` | 待实现 |

### 集成测试

| 测试项 | 描述 | 状态 |
|--------|------|------|
| 端到端流程 | CREATE → INSERT → UPDATE → DELETE → FLUSH → 验证 | 待实现 |
| 大数据量测试 | 100万行数据导入和刷新 | 待实现 |
| 故障恢复测试 | 模拟故障后自动恢复 | 待实现 |
| 性能测试 | 对比 COW/MOR 模式性能 | 待实现 |

---

## 非功能性需求

### 性能要求
- INSERT 吞吐量：>10,000 行/秒
- FLUSH 吞吐量：>50,000 行/秒
- 查询延迟：<100ms（小表）

### 可靠性要求
- 数据一致性：100%
- 故障恢复时间：<5 分钟
- 数据丢失：0

### 兼容性要求
- openGauss 9.2.x
- Apache Iceberg V3 Spec
- Parquet 格式

---

## 附录

### A. 术语表

| 术语 | 说明 |
|------|------|
| Delta 表 | ustore 内表，接收写入操作 |
| Iceberg 表 | 外表，存储最终数据 |
| FLUSH | 批量同步操作 |
| COW | Copy-On-Write，全量重写模式 |
| MOR | Merge-On-Read，增量合并模式 |
| 回表 | 从 Iceberg 读取数据到 Delta |
| 标记删除 | 记录删除操作，延迟执行 |

### B. 参考文档

- [Apache Iceberg V3 Spec](https://iceberg.apache.org/spec/)
- [openGauss 官方文档](https://docs-opengauss.osinfra.cn/zh/)
- [Parquet 格式规范](https://parquet.apache.org/docs/)
