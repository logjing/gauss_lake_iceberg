# Delta/Iceberg 双表架构插件（延迟同步模式）

## 架构说明

```
┌─────────────────────────────────────────────────────────────┐
│                      用户操作                                │
│                                                             │
│  CREATE TABLE → 注册 Delta 表（不创建 Iceberg 外表）         │
│  INSERT → 写入 Delta 内表，标记 pending_changes              │
│  ALTER TABLE → 记录 DDL 到日志表，标记 pending_changes       │
│                                                             │
│                    FLUSH 命令（用户主动触发）                 │
│                                                             │
│  1. 创建 Iceberg 外表（首次 FLUSH）                          │
│  2. 同步所有待处理的 DDL 变化                                │
│  3. 执行 INSERT INTO Iceberg SELECT * FROM Delta            │
│  4. 清理日志表，更新状态                                     │
└─────────────────────────────────────────────────────────────┘

┌─────────────┐         FLUSH          ┌─────────────────────┐
│ Delta 表    │  ──────────────────▶   │ Iceberg 外表        │
│ (ustore)    │                        │ (Parquet)           │
│             │                        │                     │
│ 实时写入    │                        │ 批量写入            │
│ DDL 操作    │                        │ FLUSH 时同步        │
└─────────────┘                        └─────────────────────┘
```

## 核心特性

### 1. 延迟同步模式
- **CREATE TABLE**：只注册 Delta 表映射，不创建 Iceberg 外表
- **INSERT**：数据写入 Delta 内表，标记 pending_changes
- **ALTER TABLE**：DDL 变化记录到日志表，不立即同步
- **DROP TABLE**：删除 Delta 表 + Iceberg 外表（如已创建）+ 映射 + 日志

### 2. FLUSH 命令
用户主动触发批量同步：
- 首次 FLUSH：创建 Iceberg 外表
- 同步所有待处理的 DDL 变化
- 执行 `INSERT INTO Iceberg SELECT * FROM Delta`
- 清理已同步的 DDL 日志

### 3. 优势
- **批量写入效率高**：攒一批数据后一次写入 Parquet
- **减少 Parquet 文件碎片**：避免每次 INSERT 产生小文件
- **DDL 批量同步**：多次 ALTER 在 FLUSH 时一次性同步
- **灵活控制**：用户决定何时同步

## 使用示例

### 1. 加载扩展

```sql
CREATE EXTENSION IF NOT EXISTS pg_lake_iceberg;  -- Iceberg FDW
CREATE EXTENSION dual_table;
```

### 2. 创建 Delta 表

```sql
CREATE SCHEMA test_delta;

-- 创建 Delta 表（不立即创建 Iceberg 外表）
CREATE TABLE test_delta.orders (
    id int PRIMARY KEY,
    customer_id int NOT NULL,
    amount float8
);

-- 查看映射状态（iceberg_table_oid 为 NULL）
SELECT * FROM gaussvector.show_delta_tables();
```

### 3. INSERT 数据（只写入 Delta 内表）

```sql
-- 单行插入
INSERT INTO test_delta.orders VALUES (1, 100, 99.5);

-- 多行插入
INSERT INTO test_delta.orders VALUES
    (2, 101, 150.0),
    (3, 102, 75.0),
    (4, 103, 200.0);

-- 数据只在 Delta 内表中
SELECT * FROM test_delta.orders;
SELECT * FROM test_delta.orders_iceberg;  -- 外表还未创建，报错
```

### 4. ALTER TABLE（记录到日志，不立即同步）

```sql
-- 添加列
ALTER TABLE test_delta.orders ADD COLUMN status text DEFAULT 'pending';

-- 修改列类型
ALTER TABLE test_delta.orders ALTER COLUMN amount TYPE numeric(10,2);

-- 查看 DDL 日志
SELECT * FROM gaussvector.delta_ddl_log 
WHERE delta_table_oid = 'test_delta.orders'::regclass AND flushed = false;
```

### 5. FLUSH 命令（批量同步到 Iceberg）

```sql
-- 执行 FLUSH
SELECT gaussvector.flush_delta_table('test_delta.orders'::regclass);

-- 返回结果：
-- {
--   "delta_table": "test_delta.orders",
--   "iceberg_table": "test_delta.orders_iceberg",
--   "rows_flushed": 4,
--   "ddl_synced": 2,
--   "flush_at": "2026-06-03 12:00:00"
-- }

-- 外表已创建，数据已同步
SELECT * FROM test_delta.orders_iceberg;

-- DDL 日志已清理
SELECT count(*) FROM gaussvector.delta_ddl_log WHERE flushed = false;
-- 结果: 0
```

### 6. 继续写入和 FLUSH

```sql
-- 继续 INSERT
INSERT INTO test_delta.orders VALUES (5, 104, 50.0, 'completed');

-- 再添加列
ALTER TABLE test_delta.orders ADD COLUMN created_at timestamp;

-- 再次 FLUSH（追加数据，同步 DDL）
SELECT gaussvector.flush_delta_table('test_delta.orders'::regclass);

-- Iceberg 外表包含所有数据
SELECT * FROM test_delta.orders_iceberg;
```

### 7. 批量 FLUSH 所有 Delta 表

```sql
-- FLUSH 所有待处理的 Delta 表
SELECT * FROM gaussvector.flush_all_delta_tables();
```

### 8. 查看状态

```sql
SELECT * FROM gaussvector.show_delta_tables();

-- 输出：
-- delta_table       | iceberg_table        | pending_changes | pending_ddl_count | last_flush_at
-- test_delta.orders | test_delta.orders... | false           | 0                 | 2026-06-03 12:00
```

## Catalog 表结构

### gaussvector.delta_tables

| 列名 | 类型 | 说明 |
|------|------|------|
| delta_table_oid | regclass | Delta 内表 OID |
| iceberg_table_oid | regclass | Iceberg 外表 OID（NULL 直到首次 FLUSH） |
| iceberg_location | text | Iceberg 存储路径 |
| pending_changes | boolean | 是否有待刷新的数据 |
| schema_version | int | Schema 版本号 |
| delta_created_at | timestamp | Delta 表创建时间 |
| last_flush_at | timestamp | 最后 FLUSH 时间 |

### gaussvector.delta_ddl_log

| 列名 | 类型 | 说明 |
|------|------|------|
| id | serial | 主键 |
| delta_table_oid | regclass | Delta 表 OID |
| ddl_type | text | DDL 类型（ADD_COLUMN/DROP_COLUMN/ALTER_TYPE 等） |
| ddl_detail | jsonb | DDL 详情 |
| created_at | timestamp | 创建时间 |
| flushed | boolean | 是否已同步 |

## 辅助函数

| 函数 | 说明 |
|------|------|
| `gaussvector.flush_delta_table(regclass)` | FLUSH 单个 Delta 表 |
| `gaussvector.flush_all_delta_tables()` | FLUSH 所有待处理的 Delta 表 |
| `gaussvector.show_delta_tables()` | 查看 Delta 表状态 |
| `gaussvector.mark_delta_pending(regclass)` | 手动标记待刷新 |
| `gaussvector.cleanup_flushed_ddl_log(int)` | 清理已同步的 DDL 日志 |

## 数据流

```
时间线：

T1: CREATE TABLE → Delta 表注册
    └─ gaussvector.delta_tables 记录映射
    └─ iceberg_table_oid = NULL

T2: INSERT → Delta 表数据增加
    └─ pending_changes = true

T3: ALTER TABLE ADD COLUMN → DDL 记录
    └─ gaussvector.delta_ddl_log 记录 DDL
    └─ pending_changes = true

T4: INSERT → Delta 表数据继续增加
    └─ pending_changes = true

T5: FLUSH → 批量同步
    ├─ 创建 Iceberg 外表（如不存在）
    ├─ 同步所有 DDL 变化
    ├─ INSERT INTO Iceberg SELECT * FROM Delta
    ├─ 清理 DDL 日志
    └─ pending_changes = false

T6: 继续 INSERT → 新数据等待下次 FLUSH
    └─ pending_changes = true

T7: 再次 FLUSH → 追加新数据
    └─ INSERT INTO Iceberg SELECT * FROM Delta
```

## 注意事项

1. **首次 FLUSH 创建外表**：Iceberg 外表在首次 FLUSH 时创建，结构与 Delta 表一致
2. **FLUSH 是追加操作**：每次 FLUSH 将 Delta 表所有数据追加到 Iceberg（不删除已有数据）
3. **DDL 批量同步**：多次 ALTER 在一次 FLUSH 中同步，减少 Parquet 文件重写次数
4. **清理旧数据**：如需清理 Delta 表数据，使用 TRUNCATE 后 FLUSH
5. **建议定期 FLUSH**：避免 Delta 表数据过多导致 FLUSH 时间过长

## 项目结构

```
/home/sin/dual_table_plugin/
├── CMakeLists.txt
├── dual_table.control
├── dual_table--1.0.sql          # SQL：Catalog 表 + FLUSH 函数
├── include/
│   ├── dual_table.h             # 核心头文件
│   └── opengauss_compat.h       # openGauss 兼容层
└── src/
    ├── dual_table_init.cpp      # Hook 安装
    ├── catalog_manager.cpp      # Catalog 操作 + DDL 日志记录
    ├── hook_process_utility.cpp # ProcessUtility Hook（DDL拦截）
    ├── hook_executor.cpp        # ExecutorRun Hook（INSERT标记）
    └── schema_sync.cpp          # 辅助函数
```

## 安装

```bash
cd /home/sin/dual_table_plugin
mkdir build && cd build
cmake ..
make

# 安装
cp dual_table.so /path/to/openGauss/lib/postgresql/
cp dual_table.control /path/to/openGauss/share/postgresql/extension/
cp dual_table--1.0.sql /path/to/openGauss/share/postgresql/extension/
```