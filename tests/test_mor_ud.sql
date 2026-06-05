-- test_mor_ud.sql - 测试 Iceberg MOR UPDATE/DELETE 级联处理
--
-- 测试流程：
-- 1. 安装扩展
-- 2. 创建 Delta 表
-- 3. INSERT 数据
-- 4. UPDATE 操作 → 记录到 delta_dml_log
-- 5. DELETE 操作 → 记录到 delta_dml_log
-- 6. 查看 DML 日志
-- 7. FLUSH 同步 → 验证 MOR/COW 模式
-- 8. 验证结果

-- ========== 1. 安装扩展 ==========
CREATE EXTENSION IF NOT EXISTS dual_table;

-- ========== 2. 创建 Delta 表（带主键）==========
DROP TABLE IF EXISTS test_mor CASCADE;
CREATE TABLE test_mor (
    id int PRIMARY KEY,
    name text,
    value numeric
);

-- ========== 3. INSERT 数据 ==========
INSERT INTO test_mor VALUES (1, 'Alice', 100.5);
INSERT INTO test_mor VALUES (2, 'Bob', 200.3);
INSERT INTO test_mor VALUES (3, 'Carol', 300.1);
INSERT INTO test_mor VALUES (4, 'David', 400.7);
INSERT INTO test_mor VALUES (5, 'Eve', 500.9);

-- 验证 Delta 表数据
SELECT * FROM test_mor ORDER BY id;

-- 验证 pending_changes 标记
SELECT delta_table_oid, pending_changes
FROM gaussvector.delta_tables
WHERE delta_table_oid = 'test_mor'::regclass;

-- ========== 4. UPDATE 操作 ==========
-- UPDATE id=1 的 name 和 value
UPDATE test_mor SET name = 'Alice_Updated', value = 150.0 WHERE id = 1;

-- UPDATE id=2 的 value
UPDATE test_mor SET value = 250.0 WHERE id = 2;

-- 验证 Delta 表 UPDATE 后的数据
SELECT * FROM test_mor ORDER BY id;

-- ========== 5. DELETE 操作 ==========
-- DELETE id=3
DELETE FROM test_mor WHERE id = 3;

-- DELETE id=5
DELETE FROM test_mor WHERE id = 5;

-- 验证 Delta 表 DELETE 后的数据
SELECT * FROM test_mor ORDER BY id;

-- ========== 6. 查看 DML 日志 ==========
SELECT id, delta_table_oid, operation_type, pk_values, source_sql, flushed
FROM gaussvector.delta_dml_log
ORDER BY id;

-- ========== 7. FLUSH 同步 ==========
-- 执行 FLUSH，将 DML 变化同步到 Iceberg
SELECT gaussvector.flush_delta_table('test_mor');

-- ========== 8. 验证 DML 日志已标记为 flushed ==========
SELECT id, delta_table_oid, operation_type, pk_values, flushed
FROM gaussvector.delta_dml_log
ORDER BY id;

-- ========== 9. 验证辅助函数 ==========
-- 测试获取主键列名
SELECT gaussvector.get_table_primary_keys('test_mor'::regclass);

-- 测试提取 WHERE 条件
SELECT gaussvector.extract_where_clause('UPDATE test_mor SET name = ''Alice_Updated'', value = 150.0 WHERE id = 1');
SELECT gaussvector.extract_where_clause('DELETE FROM test_mor WHERE id = 3');

-- ========== 10. 验证 FLUSH 结果 ==========
-- 查看 delta_tables 状态
SELECT delta_table_oid, iceberg_table_oid, pending_changes, last_flush_at
FROM gaussvector.delta_tables
WHERE delta_table_oid = 'test_mor'::regclass;

-- ========== 11. 创建无主键表测试 ==========
DROP TABLE IF EXISTS test_mor_nopk CASCADE;
CREATE TABLE test_mor_nopk (
    col_a int,
    col_b text,
    col_c numeric
);

INSERT INTO test_mor_nopk VALUES (10, 'row1', 1.1);
INSERT INTO test_mor_nopk VALUES (20, 'row2', 2.2);
INSERT INTO test_mor_nopk VALUES (30, 'row3', 3.3);

-- UPDATE 无主键表
UPDATE test_mor_nopk SET col_b = 'row1_updated' WHERE col_a = 10;

-- DELETE 无主键表
DELETE FROM test_mor_nopk WHERE col_a = 30;

-- 查看主键回退（应使用所有列）
SELECT gaussvector.get_table_primary_keys('test_mor_nopk'::regclass);

-- 查看 DML 日志
SELECT id, delta_table_oid, operation_type, pk_values, source_sql
FROM gaussvector.delta_dml_log
WHERE delta_table_oid = 'test_mor_nopk'::regclass
ORDER BY id;

-- ========== 12. 清理测试数据 ==========
-- 清理已同步的 DML 日志
SELECT gaussvector.cleanup_flushed_dml_log(0);

-- 验证清理结果
SELECT count(*) FROM gaussvector.delta_dml_log WHERE flushed = true;

-- ========== 测试完成 ==========
-- 预期结果：
-- 1. UPDATE/DELETE 操作后，delta_dml_log 应有对应记录
-- 2. pk_values 初始为 '{}'，FLUSH 时填充 WHERE 条件
-- 3. FLUSH 返回结果包含 dml_synced 和 sync_mode 字段
-- 4. 无主键表使用所有列作为标识
-- 5. 如果 Iceberg FDW 不支持 DELETE，自动回退到 COW 模式