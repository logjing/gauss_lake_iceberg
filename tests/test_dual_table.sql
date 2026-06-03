-- test_dual_table.sql - 测试双表架构插件功能

\echo '=== Dual Table Plugin Test Suite ==='

-- 加载扩展
CREATE EXTENSION IF NOT EXISTS dual_table;

-- 创建测试 schema
CREATE SCHEMA IF NOT EXISTS test_dual;

-- ==================== 测试1：CREATE TABLE 自动创建外表 ====================

\echo 'Test 1: CREATE TABLE auto-create foreign table'

CREATE TABLE test_dual.orders (
    id int PRIMARY KEY,
    customer_id int NOT NULL,
    amount float8,
    status text DEFAULT 'pending'
);

-- 验证外表是否创建
SELECT EXISTS (
    SELECT 1 FROM pg_class
    WHERE relname = 'orders_iceberg'
    AND relkind = 'f'
) AS iceberg_foreign_created;

-- 验证 catalog 映射
SELECT * FROM gaussvector.dual_tables
WHERE ustore_table_oid = 'test_dual.orders'::regclass;

-- ==================== 测试2：Schema 一致性验证 ====================

\echo 'Test 2: Schema consistency'

SELECT
    a.attname AS column_name,
    a.atttypid::regtype AS ustore_type,
    b.atttypid::regtype AS iceberg_type,
    CASE WHEN a.atttypid = b.atttypid THEN 'MATCH' ELSE 'MISMATCH' END AS type_check
FROM pg_attribute a
JOIN pg_attribute b ON a.attname = b.attname
WHERE
    a.attrelid = 'test_dual.orders'::regclass
    AND b.attrelid = 'test_dual.orders_iceberg'::regclass
    AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum;

-- ==================== 测试3：ALTER TABLE 同步 ====================

\echo 'Test 3: ALTER TABLE sync'

-- 添加列
ALTER TABLE test_dual.orders ADD COLUMN created_at timestamp;

-- 验证外表是否同步
SELECT EXISTS (
    SELECT 1 FROM pg_attribute
    WHERE attrelid = 'test_dual.orders_iceberg'::regclass
    AND attname = 'created_at'
) AS created_at_synced;

-- 修改列类型
ALTER TABLE test_dual.orders ALTER COLUMN id TYPE bigint;

-- 验证类型是否同步
SELECT
    a.atttypid::regtype AS ustore_id_type,
    b.atttypid::regtype AS iceberg_id_type
FROM pg_attribute a
JOIN pg_attribute b ON a.attname = b.attname
WHERE
    a.attrelid = 'test_dual.orders'::regclass
    AND b.attrelid = 'test_dual.orders_iceberg'::regclass
    AND a.attname = 'id';

-- ==================== 测试4：DROP TABLE 自动删除 ====================

\echo 'Test 4: DROP TABLE auto-cleanup'

-- 删除内表
DROP TABLE test_dual.orders CASCADE;

-- 验证外表是否删除
SELECT NOT EXISTS (
    SELECT 1 FROM pg_class
    WHERE relname = 'orders_iceberg'
    AND relkind = 'f'
) AS iceberg_foreign_deleted;

-- 验证 catalog 是否删除
SELECT count(*) AS catalog_entries
FROM gaussvector.dual_tables
WHERE ustore_table_oid::text LIKE '%orders%';

-- ==================== 测试5：禁用同步选项 ====================

\echo 'Test 5: Disable sync option'

CREATE TABLE test_dual.no_sync (
    id int,
    data text
) WITH (dual_table_sync = 'off');

-- 验证是否未创建外表
SELECT NOT EXISTS (
    SELECT 1 FROM pg_class
    WHERE relname = 'no_sync_iceberg'
    AND relkind = 'f'
) AS no_iceberg_created;

DROP TABLE test_dual.no_sync;

-- ==================== 清理 ====================

\echo 'Cleanup'

DROP SCHEMA test_dual CASCADE;

-- ==================== 测试结果汇总 ====================

\echo '=== Test Summary ==='

SELECT 'Dual Table Plugin Tests' AS test_name,
    'All tests completed' AS status;