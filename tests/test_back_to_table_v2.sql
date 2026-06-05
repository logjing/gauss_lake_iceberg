-- test_back_to_table_v2.sql - 测试回表机制（清晰版本）
--
-- 测试目标：验证 UPDATE/DELETE 条件匹配的行不在 Delta 时的回表行为
-- 预期：
--   第一次 UPDATE/DELETE: 返回 0 行（回表插入，但 DML 本身不匹配）
--   第二次 UPDATE/DELETE: 返回 1 行（行已在 Delta 中，DML 成功）

-- ========== 1. 安装扩展 ==========
CREATE EXTENSION IF NOT EXISTS dual_table;

-- ========== 2. 清理旧测试数据 ==========
DROP TABLE IF EXISTS test_btt_v2 CASCADE;
DROP TABLE IF EXISTS test_btt_v2_iceberg CASCADE;

-- ========== 3. 创建 Delta 表 ==========
CREATE TABLE test_btt_v2 (
    id int PRIMARY KEY,
    name text,
    value numeric
);

-- ========== 4. 注册为 Delta 表 ==========
SELECT gaussvector.register_delta_table('test_btt_v2', '/data/iceberg/test_btt_v2');

-- ========== 5. 插入初始数据到 Delta 表 ==========
INSERT INTO test_btt_v2 VALUES (1, 'Alice', 100.5);
INSERT INTO test_btt_v2 VALUES (2, 'Bob', 200.3);

-- ========== 6. 创建 Iceberg 外表（模拟） ==========
CREATE TABLE test_btt_v2_iceberg (
    id int PRIMARY KEY,
    name text,
    value numeric
);

-- 更新 delta_tables 中的 iceberg_table_oid
UPDATE gaussvector.delta_tables
SET iceberg_table_oid = 'test_btt_v2_iceberg'::regclass
WHERE delta_table_oid = 'test_btt_v2'::regclass;

-- ========== 7. 插入数据到 Iceberg 表（模拟已存在的数据） ==========
INSERT INTO test_btt_v2_iceberg VALUES (1, 'Alice', 100.5);
INSERT INTO test_btt_v2_iceberg VALUES (2, 'Bob', 200.3);
INSERT INTO test_btt_v2_iceberg VALUES (3, 'Carol', 300.1);  -- 只在 Iceberg
INSERT INTO test_btt_v2_iceberg VALUES (4, 'David', 400.7);  -- 只在 Iceberg

-- ========== 8. 验证初始状态 ==========
SELECT '========== 初始状态 ==========' AS info;
SELECT 'Delta 表:' AS label, count(*) AS cnt FROM test_btt_v2;
SELECT 'Iceberg 表:' AS label, count(*) AS cnt FROM test_btt_v2_iceberg;

-- ========== 9. 测试回表 UPDATE ==========
SELECT '========== 测试回表 UPDATE ==========' AS info;

-- 第一次 UPDATE: id=3 只在 Iceberg，触发回表
SELECT '第一次 UPDATE id=3 (只在 Iceberg):' AS test_name;
UPDATE test_btt_v2 SET name = 'Carol_Updated' WHERE id = 3;
-- 预期: UPDATE 0 (回表插入，但 UPDATE 本身不匹配)

-- 验证 Delta 表已插入 id=3
SELECT '回表后 Delta 表数据:' AS label;
SELECT * FROM test_btt_v2 ORDER BY id;

-- 第二次 UPDATE: id=3 现在在 Delta 中
SELECT '第二次 UPDATE id=3 (已在 Delta):' AS test_name;
UPDATE test_btt_v2 SET name = 'Carol_Updated2', value = 350.0 WHERE id = 3;
-- 预期: UPDATE 1

-- 验证 Delta 表数据已更新
SELECT '第二次 UPDATE 后 Delta 表数据:' AS label;
SELECT * FROM test_btt_v2 ORDER BY id;

-- ========== 10. 测试回表 DELETE ==========
SELECT '========== 测试回表 DELETE ==========' AS info;

-- 第一次 DELETE: id=4 只在 Iceberg，触发回表
SELECT '第一次 DELETE id=4 (只在 Iceberg):' AS test_name;
DELETE FROM test_btt_v2 WHERE id = 4;
-- 预期: DELETE 0 (回表插入，但 DELETE 本身不匹配)

-- 验证 Delta 表已插入 id=4
SELECT '回表后 Delta 表数据:' AS label;
SELECT * FROM test_btt_v2 ORDER BY id;

-- 第二次 DELETE: id=4 现在在 Delta 中
SELECT '第二次 DELETE id=4 (已在 Delta):' AS test_name;
DELETE FROM test_btt_v2 WHERE id = 4;
-- 预期: DELETE 1

-- 验证 Delta 表数据已删除
SELECT '第二次 DELETE 后 Delta 表数据:' AS label;
SELECT * FROM test_btt_v2 ORDER BY id;

-- ========== 11. 对比：已在 Delta 中的行 ==========
SELECT '========== 对比：已在 Delta 中的行 ==========' AS info;

SELECT 'UPDATE id=1 (已在 Delta):' AS test_name;
UPDATE test_btt_v2 SET value = 150.0 WHERE id = 1;
-- 预期: UPDATE 1

SELECT 'DELETE id=2 (已在 Delta):' AS test_name;
DELETE FROM test_btt_v2 WHERE id = 2;
-- 预期: DELETE 1

-- ========== 12. 最终状态 ==========
SELECT '========== 最终状态 ==========' AS info;
SELECT '最终 Delta 表数据:' AS label;
SELECT * FROM test_btt_v2 ORDER BY id;

SELECT 'DML 日志:' AS label;
SELECT id, operation_type, pk_values, source_sql
FROM gaussvector.delta_dml_log
WHERE delta_table_oid = 'test_btt_v2'::regclass
ORDER BY id;

SELECT 'Iceberg 删除日志:' AS label;
SELECT id, pk_values, row_data
FROM gaussvector.delta_iceberg_delete_log
WHERE delta_table_oid = 'test_btt_v2'::regclass
ORDER BY id;

-- ========== 13. 清理 ==========
SELECT gaussvector.unregister_delta_table('test_btt_v2');
DROP TABLE IF EXISTS test_btt_v2 CASCADE;
DROP TABLE IF EXISTS test_btt_v2_iceberg CASCADE;

SELECT '========== 测试完成 ==========' AS info;
