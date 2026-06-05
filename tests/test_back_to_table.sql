-- test_back_to_table.sql - 测试回表机制
--
-- 测试场景：UPDATE/DELETE 条件匹配的行不在 Delta 表中，而在 Iceberg 表中
-- 预期行为：
-- 1. 触发器自动从 Iceberg 读取数据并插入 Delta 表
-- 2. 标记 Iceberg 行已删除
-- 3. 第二次 UPDATE/DELETE 成功修改数据

-- ========== 1. 安装扩展 ==========
CREATE EXTENSION IF NOT EXISTS dual_table;

-- ========== 2. 清理旧测试数据 ==========
DROP TABLE IF EXISTS test_back_to_table CASCADE;

-- ========== 3. 创建 Delta 表 ==========
CREATE TABLE test_back_to_table (
    id int PRIMARY KEY,
    name text,
    value numeric
);

-- ========== 4. 注册为 Delta 表 ==========
SELECT gaussvector.register_delta_table('test_back_to_table', '/data/iceberg/test_back_to_table');

-- ========== 5. 插入初始数据到 Delta 表 ==========
INSERT INTO test_back_to_table VALUES (1, 'Alice', 100.5);
INSERT INTO test_back_to_table VALUES (2, 'Bob', 200.3);

-- ========== 6. 创建 Iceberg 外表（模拟） ==========
-- 注意：实际环境需要 Iceberg FDW，这里使用普通表模拟
DROP TABLE IF EXISTS test_back_to_table_iceberg CASCADE;
CREATE TABLE test_back_to_table_iceberg (
    id int PRIMARY KEY,
    name text,
    value numeric
);

-- 更新 delta_tables 中的 iceberg_table_oid
UPDATE gaussvector.delta_tables
SET iceberg_table_oid = 'test_back_to_table_iceberg'::regclass
WHERE delta_table_oid = 'test_back_to_table'::regclass;

-- ========== 7. 插入数据到 Iceberg 表（模拟已存在的数据） ==========
INSERT INTO test_back_to_table_iceberg VALUES (1, 'Alice', 100.5);
INSERT INTO test_back_to_table_iceberg VALUES (2, 'Bob', 200.3);
INSERT INTO test_back_to_table_iceberg VALUES (3, 'Carol', 300.1);  -- 这行只在 Iceberg 中
INSERT INTO test_back_to_table_iceberg VALUES (4, 'David', 400.7);  -- 这行只在 Iceberg 中

-- ========== 8. 验证初始状态 ==========
SELECT '=== Delta 表初始数据 ===' AS info;
SELECT * FROM test_back_to_table ORDER BY id;

SELECT '=== Iceberg 表初始数据 ===' AS info;
SELECT * FROM test_back_to_table_iceberg ORDER BY id;

SELECT '=== 触发器状态 ===' AS info;
SELECT tgname, tgenabled FROM pg_trigger
WHERE tgrelid = 'test_back_to_table'::regclass
AND tgname LIKE 'delta_dml%';

-- ========== 9. 测试回表：UPDATE 不存在于 Delta 的行 ==========
SELECT '=== 测试回表: UPDATE id=3 (只在 Iceberg 中) ===' AS info;

-- 第一次 UPDATE：触发器从 Iceberg 读取 id=3 到 Delta，但 UPDATE 本身返回 0 行
UPDATE test_back_to_table SET name = 'Carol_Updated' WHERE id = 3;

-- 验证 Delta 表已插入 id=3（回表成功）
SELECT '=== Delta 表第一次 UPDATE 后数据 ===' AS info;
SELECT * FROM test_back_to_table ORDER BY id;

-- 第二次 UPDATE：现在 id=3 已在 Delta 中，应该成功更新
UPDATE test_back_to_table SET name = 'Carol_Updated2', value = 350.0 WHERE id = 3;

SELECT '=== Delta 表第二次 UPDATE 后数据 ===' AS info;
SELECT * FROM test_back_to_table ORDER BY id;

-- ========== 10. 测试回表：DELETE 不存在于 Delta 的行 ==========
SELECT '=== 测试回表: DELETE id=4 (只在 Iceberg 中) ===' AS info;

-- 第一次 DELETE：触发器从 Iceberg 读取 id=4 到 Delta，但 DELETE 本身返回 0 行
DELETE FROM test_back_to_table WHERE id = 4;

-- 验证 Delta 表已插入 id=4（回表成功）
SELECT '=== Delta 表第一次 DELETE 后数据 ===' AS info;
SELECT * FROM test_back_to_table ORDER BY id;

-- 第二次 DELETE：现在 id=4 已在 Delta 中，应该成功删除
DELETE FROM test_back_to_table WHERE id = 4;

SELECT '=== Delta 表第二次 DELETE 后数据 ===' AS info;
SELECT * FROM test_back_to_table ORDER BY id;

-- ========== 11. 验证 DML 日志 ==========
SELECT '=== DML 日志 ===' AS info;
SELECT id, delta_table_oid, operation_type, pk_values, source_sql
FROM gaussvector.delta_dml_log
ORDER BY id;

-- ========== 12. 测试对已在 Delta 中的行进行 UPDATE ==========
SELECT '=== 测试 UPDATE 已在 Delta 中的行 ===' AS info;

-- id=1 已在 Delta 中，应该直接更新
UPDATE test_back_to_table SET value = 150.0 WHERE id = 1;

SELECT '=== Delta 表最终数据 ===' AS info;
SELECT * FROM test_back_to_table ORDER BY id;

-- ========== 13. 测试对已在 Delta 中的行进行 DELETE ==========
SELECT '=== 测试 DELETE 已在 Delta 中的行 ===' AS info;

-- id=2 已在 Delta 中，应该直接删除
DELETE FROM test_back_to_table WHERE id = 2;

SELECT '=== Delta 表最终数据 ===' AS info;
SELECT * FROM test_back_to_table ORDER BY id;

-- ========== 14. 验证最终日志状态 ==========
SELECT '=== 最终 DML 日志 ===' AS info;
SELECT id, operation_type, pk_values, flushed
FROM gaussvector.delta_dml_log
ORDER BY id;

SELECT '=== 最终 Iceberg 删除日志 ===' AS info;
SELECT id, delta_table_oid, pk_values, flushed
FROM gaussvector.delta_iceberg_delete_log
ORDER BY id;

-- ========== 15. 清理 ==========
-- SELECT gaussvector.unregister_delta_table('test_back_to_table');
-- DROP TABLE IF EXISTS test_back_to_table CASCADE;
-- DROP TABLE IF EXISTS test_back_to_table_iceberg CASCADE;

-- ========== 测试完成 ==========
SELECT '=== 回表测试完成 ===' AS info;
