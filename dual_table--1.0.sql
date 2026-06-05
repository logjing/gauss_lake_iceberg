-- dual_table--1.0.sql - Delta/Iceberg 双表架构（延迟同步模式）
--
-- 架构说明：
-- - Delta 表（ustore 内表）：接收所有 INSERT 和 DDL 操作
-- - Iceberg 表（外表）：存储最终数据（Parquet 格式）
-- - FLUSH 命令：用户主动触发，批量同步数据和结构
-- - UPDATE/DELETE 操作记录到 DML 日志，FLUSH 时同步到 Iceberg

-- 创建 schema
CREATE SCHEMA IF NOT EXISTS gaussvector;

-- ==================== Catalog 映射表 ====================

-- Delta 表和 Iceberg 表的映射关系
CREATE TABLE gaussvector.delta_tables (
    delta_table_oid regclass PRIMARY KEY,
    iceberg_table_oid regclass,
    iceberg_location text,
    delta_created_at timestamp DEFAULT current_timestamp,
    last_flush_at timestamp,
    pending_changes boolean DEFAULT true,
    -- DDL 变化追踪
    schema_version int DEFAULT 1,
    pending_ddl_changes text[] DEFAULT '{}'
);

COMMENT ON TABLE gaussvector.delta_tables IS 'Delta/Iceberg dual table mapping with delayed sync';
COMMENT ON COLUMN gaussvector.delta_tables.delta_table_oid IS 'Delta table (ustore internal table) OID';
COMMENT ON COLUMN gaussvector.delta_tables.iceberg_table_oid IS 'Iceberg table (foreign table) OID, NULL until flushed';
COMMENT ON COLUMN gaussvector.delta_tables.pending_changes IS 'Whether delta table has pending data to flush';
COMMENT ON COLUMN gaussvector.delta_tables.pending_ddl_changes IS 'List of pending DDL changes to sync';

-- ==================== DDL 变化记录表 ====================

-- 记录每次 DDL 操作，FLUSH 时批量同步
CREATE TABLE gaussvector.delta_ddl_log (
    id serial PRIMARY KEY,
    delta_table_oid regclass NOT NULL,
    ddl_type text NOT NULL,  -- 'ADD_COLUMN', 'DROP_COLUMN', 'ALTER_TYPE', etc.
    ddl_detail jsonb NOT NULL,  -- 具体DDL信息
    created_at timestamp DEFAULT current_timestamp,
    flushed boolean DEFAULT false
);

CREATE INDEX idx_delta_ddl_log_table ON gaussvector.delta_ddl_log(delta_table_oid);
CREATE INDEX idx_delta_ddl_log_unflushed ON gaussvector.delta_ddl_log(delta_table_oid, flushed) WHERE flushed = false;

COMMENT ON TABLE gaussvector.delta_ddl_log IS 'Log of DDL changes on delta tables, to be synced on FLUSH';

-- ==================== DML 变化记录表 ====================

-- 记录 UPDATE/DELETE 操作，FLUSH 时批量同步到 Iceberg
CREATE TABLE gaussvector.delta_dml_log (
    id serial PRIMARY KEY,
    delta_table_oid regclass NOT NULL,
    operation_type text NOT NULL,  -- 'UPDATE', 'DELETE'
    pk_values jsonb NOT NULL DEFAULT '{}',  -- 受影响行的主键值（FLUSH 时解析填充）
    new_values jsonb,              -- UPDATE 时的新值（DELETE 为 NULL）
    source_sql text,               -- 原始 SQL 语句
    created_at timestamp DEFAULT current_timestamp,
    flushed boolean DEFAULT false
);

CREATE INDEX idx_delta_dml_log_table ON gaussvector.delta_dml_log(delta_table_oid);
CREATE INDEX idx_delta_dml_log_unflushed ON gaussvector.delta_dml_log(delta_table_oid, flushed) WHERE flushed = false;

COMMENT ON TABLE gaussvector.delta_dml_log IS 'Log of DML changes (UPDATE/DELETE) on delta tables, to be synced on FLUSH';
COMMENT ON COLUMN gaussvector.delta_dml_log.operation_type IS 'DML operation type: UPDATE or DELETE';
COMMENT ON COLUMN gaussvector.delta_dml_log.pk_values IS 'Primary key values of affected rows (JSONB, populated during FLUSH)';
COMMENT ON COLUMN gaussvector.delta_dml_log.new_values IS 'New row values for UPDATE operations (NULL for DELETE)';
COMMENT ON COLUMN gaussvector.delta_dml_log.source_sql IS 'Original SQL statement that triggered the DML operation';

-- ==================== Iceberg 标记删除记录表 ====================

-- 记录 Iceberg 表中标记删除的行（回表时使用）
CREATE TABLE gaussvector.delta_iceberg_delete_log (
    id serial PRIMARY KEY,
    delta_table_oid regclass NOT NULL,
    iceberg_table_oid regclass NOT NULL,
    pk_values jsonb NOT NULL,  -- 被删除行的主键值
    row_data jsonb,            -- 被删除行的完整数据（用于回表）
    created_at timestamp DEFAULT current_timestamp,
    flushed boolean DEFAULT false
);

CREATE INDEX idx_iceberg_delete_log_table ON gaussvector.delta_iceberg_delete_log(delta_table_oid);
CREATE INDEX idx_iceberg_delete_log_unflushed ON gaussvector.delta_iceberg_delete_log(delta_table_oid, flushed) WHERE flushed = false;

COMMENT ON TABLE gaussvector.delta_iceberg_delete_log IS 'Log of rows marked as deleted in Iceberg tables (for back-to-table operations)';

-- ==================== 回表辅助函数 ====================

-- 检查行是否存在于 Delta 表中
CREATE OR REPLACE FUNCTION gaussvector.check_row_in_delta(
    delta_table regclass,
    pk_values jsonb
)
RETURNS boolean
AS $$
DECLARE
    where_clause text;
    check_sql text;
    row_exists boolean;
BEGIN
    where_clause := gaussvector.build_pk_where_clause(pk_values);

    IF where_clause IS NULL OR where_clause = '' THEN
        RETURN false;
    END IF;

    check_sql := 'SELECT EXISTS(SELECT 1 FROM ' || delta_table::text || ' WHERE ' || where_clause || ')';
    EXECUTE check_sql INTO row_exists;

    RETURN row_exists;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION gaussvector.check_row_in_delta(regclass, jsonb) IS 'Check if a row exists in Delta table by primary key';

-- 从 Iceberg 表读取行数据（回表操作）
CREATE OR REPLACE FUNCTION gaussvector.fetch_from_iceberg(
    iceberg_table regclass,
    pk_values jsonb,
    col_names text[]
)
RETURNS jsonb
AS $$
DECLARE
    where_clause text;
    fetch_sql text;
    rec record;
    result jsonb := '{}'::jsonb;
    col_name text;
    col_value text;
BEGIN
    where_clause := gaussvector.build_pk_where_clause(pk_values);

    IF where_clause IS NULL OR where_clause = '' THEN
        RAISE EXCEPTION 'Cannot build WHERE clause for fetch';
    END IF;

    fetch_sql := 'SELECT * FROM ' || iceberg_table::text || ' WHERE ' || where_clause || ' LIMIT 1';

    FOR rec IN EXECUTE fetch_sql
    LOOP
        FOREACH col_name IN ARRAY col_names
        LOOP
            EXECUTE format('SELECT ($1).%I::text', col_name)
            INTO col_value
            USING rec;

            IF col_value IS NOT NULL THEN
                result := result || ('{"' || col_name || '": "' || col_value || '"}')::jsonb;
            END IF;
        END LOOP;
    END LOOP;

    RETURN result;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION gaussvector.fetch_from_iceberg(regclass, jsonb, text[]) IS 'Fetch row data from Iceberg table (back-to-table operation)';

-- 标记 Iceberg 行已删除
CREATE OR REPLACE FUNCTION gaussvector.mark_iceberg_deleted(
    delta_table regclass,
    iceberg_table regclass,
    pk_values jsonb,
    row_data jsonb
)
RETURNS void
AS $$
BEGIN
    INSERT INTO gaussvector.delta_iceberg_delete_log
        (delta_table_oid, iceberg_table_oid, pk_values, row_data)
    VALUES
        (delta_table, iceberg_table, pk_values, row_data);

    RAISE LOG 'Marked row as deleted in Iceberg: delta=%, iceberg=%, pk=%',
        delta_table::text, iceberg_table::text, pk_values::text;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION gaussvector.mark_iceberg_deleted(regclass, regclass, jsonb, jsonb) IS 'Mark a row as deleted in Iceberg table';

-- 将行插入 Delta 表（回表后使用）
CREATE OR REPLACE FUNCTION gaussvector.insert_into_delta(
    delta_table regclass,
    col_names text[],
    row_data jsonb
)
RETURNS void
AS $$
DECLARE
    insert_sql text;
    col_list text;
    val_list text := '';
    col_name text;
    col_value text;
    first boolean := true;
BEGIN
    col_list := array_to_string(col_names, ', ');

    FOREACH col_name IN ARRAY col_names
    LOOP
        col_value := row_data->>col_name;

        IF first THEN
            first := false;
        ELSE
            val_list := val_list || ', ';
        END IF;

        IF col_value IS NULL THEN
            val_list := val_list || 'NULL';
        ELSE
            -- 尝试数值转换，否则用引号
            BEGIN
                PERFORM col_value::numeric;
                val_list := val_list || col_value;
            EXCEPTION WHEN OTHERS THEN
                val_list := val_list || quote_literal(col_value);
            END;
        END IF;
    END LOOP;

    insert_sql := 'INSERT INTO ' || delta_table::text || ' (' || col_list || ') VALUES (' || val_list || ')';
    EXECUTE insert_sql;

    RAISE LOG 'Inserted row into Delta table: %', delta_table::text;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION gaussvector.insert_into_delta(regclass, text[], jsonb) IS 'Insert a row into Delta table (after fetch from Iceberg)';

-- ==================== Delta 表注册函数 ====================

-- 注册 Delta 表（纯 SQL 实现，不依赖 C 函数）
CREATE OR REPLACE FUNCTION gaussvector.register_delta_table(
    delta_table regclass,
    location text
)
RETURNS void
AS $$
BEGIN
    -- 更新已存在的记录
    UPDATE gaussvector.delta_tables
    SET iceberg_location = location,
        pending_changes = true
    WHERE delta_table_oid = delta_table;

    -- 如果不存在则插入
    IF NOT FOUND THEN
        INSERT INTO gaussvector.delta_tables
            (delta_table_oid, iceberg_location, pending_changes)
        VALUES
            (delta_table, location, true);
    END IF;

    -- 创建 DML 触发器
    PERFORM gaussvector.create_delta_trigger(delta_table);

    RAISE LOG 'Registered delta table % with location %', delta_table::text, location;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION gaussvector.register_delta_table(regclass, text) IS 'Register a table as Delta table and create DML trigger';

-- 删除 Delta 表注册
CREATE OR REPLACE FUNCTION gaussvector.unregister_delta_table(
    delta_table regclass
)
RETURNS void
AS $$
BEGIN
    -- 删除触发器
    PERFORM gaussvector.drop_delta_trigger(delta_table);

    -- 删除 DDL 日志
    DELETE FROM gaussvector.delta_ddl_log WHERE delta_table_oid = delta_table;

    -- 删除 DML 日志
    DELETE FROM gaussvector.delta_dml_log WHERE delta_table_oid = delta_table;

    -- 删除映射
    DELETE FROM gaussvector.delta_tables WHERE delta_table_oid = delta_table;

    RAISE LOG 'Unregistered delta table %', delta_table::text;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION gaussvector.unregister_delta_table(regclass) IS 'Unregister a delta table';

-- 标记 Delta 表有待刷新数据
CREATE OR REPLACE FUNCTION gaussvector.mark_table_pending(
    delta_table regclass
)
RETURNS void
AS $$
BEGIN
    UPDATE gaussvector.delta_tables
    SET pending_changes = true
    WHERE delta_table_oid = delta_table;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Delta table % not found', delta_table::text;
    END IF;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION gaussvector.mark_table_pending(regclass) IS 'Mark delta table as having pending changes';

-- ==================== DML 同步函数 ====================

-- 从 Iceberg 表删除旧行（按主键或 WHERE 条件）
CREATE OR REPLACE FUNCTION gaussvector.sync_dml_delete(
    iceberg_table regclass,
    pk_values jsonb
)
RETURNS boolean
AS $$
DECLARE
    where_clause text;
    delete_sql text;
BEGIN
    -- 检查是否有 where_clause
    IF pk_values ? 'where_clause' THEN
        where_clause := pk_values->>'where_clause';
    ELSE
        -- 构建主键 WHERE 子句
        where_clause := gaussvector.build_pk_where_clause(pk_values);
    END IF;

    IF where_clause IS NULL OR where_clause = '' THEN
        RAISE EXCEPTION 'Cannot build WHERE clause for DELETE';
    END IF;

    -- 执行 DELETE
    delete_sql := 'DELETE FROM ' || iceberg_table::text || ' WHERE ' || where_clause;
    EXECUTE delete_sql;

    RAISE LOG 'DML Sync: DELETE from % WHERE %', iceberg_table::text, where_clause;
    RETURN true;
EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'DML Sync: DELETE failed for % - %', iceberg_table::text, SQLERRM;
    RETURN false;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION gaussvector.sync_dml_delete(regclass, jsonb) IS 'Delete rows from Iceberg table by primary key or WHERE clause';

-- 更新 Iceberg 表中的行（DELETE 旧行 + INSERT 新行）
CREATE OR REPLACE FUNCTION gaussvector.sync_dml_update(
    iceberg_table regclass,
    delta_table regclass,
    col_names text[],
    pk_values jsonb
)
RETURNS boolean
AS $$
DECLARE
    where_clause text;
    delete_sql text;
    insert_sql text;
    col_list text;
BEGIN
    -- 检查是否有 where_clause
    IF pk_values ? 'where_clause' THEN
        where_clause := pk_values->>'where_clause';
    ELSE
        -- 构建主键 WHERE 子句
        where_clause := gaussvector.build_pk_where_clause(pk_values);
    END IF;

    IF where_clause IS NULL OR where_clause = '' THEN
        RAISE EXCEPTION 'Cannot build WHERE clause for UPDATE';
    END IF;

    col_list := array_to_string(col_names, ', ');

    -- DELETE 旧行
    delete_sql := 'DELETE FROM ' || iceberg_table::text || ' WHERE ' || where_clause;
    EXECUTE delete_sql;

    -- INSERT 新行（从 Delta 表读取最新数据）
    insert_sql := 'INSERT INTO ' || iceberg_table::text || ' (' || col_list || ') '
                  'SELECT ' || col_list || ' FROM ' || delta_table::text || ' WHERE ' || where_clause;
    EXECUTE insert_sql;

    RAISE LOG 'DML Sync: UPDATE in % WHERE %', iceberg_table::text, where_clause;
    RETURN true;
EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'DML Sync: UPDATE failed for % - %', iceberg_table::text, SQLERRM;
    RETURN false;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION gaussvector.sync_dml_update(regclass, regclass, text[], jsonb) IS 'Update rows in Iceberg table (DELETE old + INSERT new)';

-- 构建主键 WHERE 子句
CREATE OR REPLACE FUNCTION gaussvector.build_pk_where_clause(
    pk_values jsonb
)
RETURNS text
AS $$
DECLARE
    result text := '';
    key text;
    value text;
    first boolean := true;
BEGIN
    FOR key, value IN SELECT * FROM jsonb_each_text(pk_values)
    LOOP
        IF first THEN
            first := false;
        ELSE
            result := result || ' AND ';
        END IF;

        -- 尝试数值转换，否则用引号
        BEGIN
            PERFORM value::numeric;
            result := result || quote_ident(key) || ' = ' || value;
        EXCEPTION WHEN OTHERS THEN
            result := result || quote_ident(key) || ' = ' || quote_literal(value);
        END;
    END LOOP;

    RETURN result;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION gaussvector.build_pk_where_clause(jsonb) IS 'Build WHERE clause from JSONB primary key values';

-- ==================== 回表处理函数 ====================

-- 处理回表操作：从 Iceberg 读取数据并插入 Delta 表
-- 当 UPDATE/DELETE 的 WHERE 条件匹配的行不在 Delta 表中时调用
CREATE OR REPLACE FUNCTION gaussvector.process_back_to_table(
    delta_table regclass,
    source_sql text
)
RETURNS int
AS $$
DECLARE
    iceberg_oid regclass;
    iceberg_location text;
    col_names text[];
    where_clause text;
    fetch_sql text;
    rec record;
    pk_values jsonb;
    row_data jsonb;
    inserted_count int := 0;
    col_name text;
    col_value text;
BEGIN
    -- 获取 Iceberg 表信息
    SELECT iceberg_table_oid, iceberg_location INTO iceberg_oid, iceberg_location
    FROM gaussvector.delta_tables
    WHERE delta_table_oid = delta_table;

    -- 如果没有 Iceberg 表，直接返回
    IF iceberg_oid IS NULL THEN
        RETURN 0;
    END IF;

    -- 获取列名
    SELECT array_agg(quote_ident(attname) ORDER BY attnum) INTO col_names
    FROM pg_attribute
    WHERE attrelid = delta_table AND attnum > 0 AND NOT attisdropped;

    -- 从 source_sql 提取 WHERE 条件
    where_clause := gaussvector.extract_where_clause(source_sql);

    -- 如果没有 WHERE 条件，无法回表
    IF where_clause IS NULL OR where_clause = '' THEN
        RETURN 0;
    END IF;

    -- 从 Iceberg 表读取匹配的行
    fetch_sql := 'SELECT * FROM ' || iceberg_oid::text || ' WHERE ' || where_clause;

    FOR rec IN EXECUTE fetch_sql
    LOOP
        -- 构建主键值
        pk_values := '{}'::jsonb;
        row_data := '{}'::jsonb;

        FOREACH col_name IN ARRAY col_names
        LOOP
            EXECUTE format('SELECT ($1).%I::text', col_name)
            INTO col_value
            USING rec;

            IF col_value IS NOT NULL THEN
                row_data := row_data || ('{"' || col_name || '": "' || col_value || '"}')::jsonb;
            END IF;
        END LOOP;

        -- 使用第一列作为主键（简化处理）
        EXECUTE format('SELECT ($1).%I::text', col_names[1])
        INTO col_value
        USING rec;

        pk_values := ('{"' || col_names[1] || '": "' || col_value || '"}')::jsonb;

        -- 检查是否已在 Delta 表中
        IF NOT gaussvector.check_row_in_delta(delta_table, pk_values) THEN
            -- 标记 Iceberg 行已删除
            PERFORM gaussvector.mark_iceberg_deleted(delta_table, iceberg_oid, pk_values, row_data);

            -- 插入 Delta 表
            PERFORM gaussvector.insert_into_delta(delta_table, col_names, row_data);

            inserted_count := inserted_count + 1;
        END IF;
    END LOOP;

    IF inserted_count > 0 THEN
        RAISE LOG 'Back-to-table: inserted % rows from Iceberg to Delta', inserted_count;
    END IF;

    RETURN inserted_count;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION gaussvector.process_back_to_table(regclass, text) IS 'Process back-to-table: fetch rows from Iceberg and insert into Delta';

-- ==================== DML 触发器支持 ====================

-- 语句级触发器函数：在 UPDATE/DELETE 之前检查是否需要回表
CREATE OR REPLACE FUNCTION gaussvector.delta_dml_before_stmt_func()
RETURNS trigger
AS $$
DECLARE
    delta_table_oid regclass;
    source_sql text;
    back_count int;
BEGIN
    delta_table_oid := TG_RELID::regclass;
    source_sql := current_query();

    -- 处理回表操作
    back_count := gaussvector.process_back_to_table(delta_table_oid, source_sql);

    IF back_count > 0 THEN
        RAISE NOTICE 'Back-to-table: % rows fetched from Iceberg to Delta', back_count;
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION gaussvector.delta_dml_before_stmt_func() IS 'Before statement trigger: process back-to-table if needed';

-- 行级触发器函数：记录 DML 操作到日志
CREATE OR REPLACE FUNCTION gaussvector.delta_dml_trigger_func()
RETURNS trigger
AS $$
DECLARE
    delta_table_oid regclass;
    pk_json text := '{}';
BEGIN
    delta_table_oid := TG_RELID::regclass;

    IF TG_OP = 'INSERT' THEN
        -- INSERT: 仅标记 pending_changes
        UPDATE gaussvector.delta_tables
        SET pending_changes = true
        WHERE delta_table_oid = delta_table_oid;

        RETURN NEW;

    ELSIF TG_OP = 'UPDATE' THEN
        -- UPDATE: 记录旧行主键值到 delta_dml_log
        pk_json := '{"id": ' || OLD.id::text || '}';

        INSERT INTO gaussvector.delta_dml_log
            (delta_table_oid, operation_type, pk_values, new_values, source_sql)
        VALUES
            (delta_table_oid, 'UPDATE', pk_json::jsonb, NULL, current_query());

        UPDATE gaussvector.delta_tables
        SET pending_changes = true
        WHERE delta_table_oid = delta_table_oid;

        RETURN NEW;

    ELSIF TG_OP = 'DELETE' THEN
        -- DELETE: 记录旧行主键值到 delta_dml_log
        pk_json := '{"id": ' || OLD.id::text || '}';

        INSERT INTO gaussvector.delta_dml_log
            (delta_table_oid, operation_type, pk_values, new_values, source_sql)
        VALUES
            (delta_table_oid, 'DELETE', pk_json::jsonb, NULL, current_query());

        UPDATE gaussvector.delta_tables
        SET pending_changes = true
        WHERE delta_table_oid = delta_table_oid;

        RETURN OLD;
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION gaussvector.delta_dml_trigger_func() IS 'Trigger function to intercept DML operations on delta tables';

-- 创建触发器的辅助函数
CREATE OR REPLACE FUNCTION gaussvector.create_delta_trigger(
    delta_table regclass
)
RETURNS void
AS $$
DECLARE
    delta_schema text;
    delta_name text;
    trigger_name text;
    stmt_trigger_name text;
BEGIN
    delta_schema := split_part(delta_table::text, '.', 1);
    delta_name := split_part(delta_table::text, '.', 2);
    trigger_name := 'delta_dml_trigger_' || delta_name;
    stmt_trigger_name := 'delta_dml_before_stmt_' || delta_name;

    -- 删除已存在的触发器（如果存在）
    EXECUTE format(
        'DROP TRIGGER IF EXISTS %I ON %I.%I',
        trigger_name, delta_schema, delta_name
    );
    EXECUTE format(
        'DROP TRIGGER IF EXISTS %I ON %I.%I',
        stmt_trigger_name, delta_schema, delta_name
    );

    -- 创建语句级触发器：BEFORE UPDATE/DELETE（处理回表）
    EXECUTE format(
        'CREATE TRIGGER %I
         BEFORE UPDATE OR DELETE ON %I.%I
         FOR EACH STATEMENT
         EXECUTE FUNCTION gaussvector.delta_dml_before_stmt_func()',
        stmt_trigger_name, delta_schema, delta_name
    );

    -- 创建行级触发器：AFTER INSERT/UPDATE/DELETE（记录日志）
    EXECUTE format(
        'CREATE TRIGGER %I
         AFTER INSERT OR UPDATE OR DELETE ON %I.%I
         FOR EACH ROW
         EXECUTE FUNCTION gaussvector.delta_dml_trigger_func()',
        trigger_name, delta_schema, delta_name
    );

    RAISE LOG 'Delta: Created DML triggers on table % (before_stmt + after_row)', delta_table::text;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION gaussvector.create_delta_trigger(regclass) IS 'Create DML trigger on delta table for INSERT/UPDATE/DELETE interception';

-- 删除触发器的辅助函数
CREATE OR REPLACE FUNCTION gaussvector.drop_delta_trigger(
    delta_table regclass
)
RETURNS void
AS $$
DECLARE
    delta_schema text;
    delta_name text;
    trigger_name text;
    stmt_trigger_name text;
BEGIN
    delta_schema := split_part(delta_table::text, '.', 1);
    delta_name := split_part(delta_table::text, '.', 2);
    trigger_name := 'delta_dml_trigger_' || delta_name;
    stmt_trigger_name := 'delta_dml_before_stmt_' || delta_name;

    EXECUTE format(
        'DROP TRIGGER IF EXISTS %I ON %I.%I',
        trigger_name, delta_schema, delta_name
    );
    EXECUTE format(
        'DROP TRIGGER IF EXISTS %I ON %I.%I',
        stmt_trigger_name, delta_schema, delta_name
    );

    RAISE LOG 'Delta: Dropped DML triggers from table %', delta_table::text;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION gaussvector.drop_delta_trigger(regclass) IS 'Drop DML trigger from delta table';

-- ==================== Iceberg 外部服务器 ====================

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_foreign_server WHERE srvname = 'iceberg_server') THEN
        IF EXISTS (SELECT 1 FROM pg_foreign_data_wrapper WHERE fdwname = 'pg_lake_iceberg') THEN
            CREATE SERVER iceberg_server FOREIGN DATA WRAPPER pg_lake_iceberg;
        ELSIF EXISTS (SELECT 1 FROM pg_foreign_data_wrapper WHERE fdwname = 'iceberg_fdw') THEN
            CREATE SERVER iceberg_server FOREIGN DATA WRAPPER iceberg_fdw;
        ELSE
            CREATE SERVER iceberg_server FOREIGN DATA WRAPPER file_fdw;
            RAISE WARNING 'No Iceberg FDW found, using file_fdw (limited functionality)';
        END IF;
    END IF;
END
$$;

-- ==================== 核心函数：FLUSH ====================

-- DDL 同步处理函数（包含 FOR LOOP）
CREATE OR REPLACE FUNCTION gaussvector.sync_all_ddl_changes(
    p_delta_table regclass,
    p_iceberg_oid regclass
)
RETURNS int
AS $$
DECLARE
    ddl_record record;
    ddl_count int;
BEGIN
    SELECT count(*) INTO ddl_count
    FROM gaussvector.delta_ddl_log
    WHERE delta_table_oid = p_delta_table AND flushed = false;

    FOR ddl_record IN
        SELECT id, ddl_type, ddl_detail
        FROM gaussvector.delta_ddl_log
        WHERE delta_table_oid = p_delta_table AND flushed = false
        ORDER BY created_at
    LOOP
        PERFORM gaussvector.sync_ddl_change(p_iceberg_oid, ddl_record.ddl_type, ddl_record.ddl_detail);
    END LOOP;

    -- 标记所有已处理的 DDL 为 flushed
    UPDATE gaussvector.delta_ddl_log SET flushed = true
    WHERE delta_table_oid = p_delta_table AND flushed = false;

    RETURN ddl_count;
END;
$$ LANGUAGE plpgsql;

-- DML 同步处理函数（包含 FOR LOOP）
CREATE OR REPLACE FUNCTION gaussvector.sync_all_dml_changes(
    p_delta_table regclass,
    p_iceberg_oid regclass,
    p_col_names text[],
    p_batch_size int DEFAULT 10000
)
RETURNS jsonb
AS $$
DECLARE
    dml_record record;
    dml_count int;
    pk_cols text[];
    where_clause text;
    pk_json jsonb;
    mor_success boolean := true;
    need_cow_rebuild boolean := false;
    result_json jsonb;
BEGIN
    pk_cols := gaussvector.get_table_primary_keys(p_delta_table);

    FOR dml_record IN
        SELECT id, operation_type, pk_values, new_values, source_sql
        FROM gaussvector.delta_dml_log
        WHERE delta_table_oid = p_delta_table AND flushed = false
        ORDER BY created_at
    LOOP
        pk_json := dml_record.pk_values;

        IF pk_json = '{}'::jsonb OR pk_json IS NULL THEN
            where_clause := gaussvector.extract_where_clause(dml_record.source_sql);
            IF where_clause IS NOT NULL THEN
                pk_json := jsonb_build_object('where_clause', where_clause);
                UPDATE gaussvector.delta_dml_log SET pk_values = pk_json WHERE id = dml_record.id;
            ELSE
                need_cow_rebuild := true;
            END IF;
        END IF;

        IF dml_record.operation_type = 'DELETE' THEN
            BEGIN
                mor_success := gaussvector.sync_dml_delete(p_iceberg_oid, pk_json);
            EXCEPTION WHEN OTHERS THEN
                mor_success := false;
                need_cow_rebuild := true;
            END;
        ELSIF dml_record.operation_type = 'UPDATE' THEN
            BEGIN
                mor_success := gaussvector.sync_dml_update(p_iceberg_oid, p_delta_table, p_col_names, pk_json);
            EXCEPTION WHEN OTHERS THEN
                need_cow_rebuild := true;
            END;
        END IF;

        UPDATE gaussvector.delta_dml_log SET flushed = true WHERE id = dml_record.id;
    END LOOP;

    SELECT count(*) INTO dml_count
    FROM gaussvector.delta_dml_log
    WHERE delta_table_oid = p_delta_table AND flushed = true;

    result_json := jsonb_build_object(
        'dml_count', dml_count,
        'mor_success', mor_success,
        'need_cow_rebuild', need_cow_rebuild
    );

    RETURN result_json;
END;
$$ LANGUAGE plpgsql;

-- 数据同步处理函数（包含 WHILE LOOP）
CREATE OR REPLACE FUNCTION gaussvector.sync_data_to_iceberg(
    p_delta_table regclass,
    p_iceberg_oid regclass,
    p_col_names text[],
    p_batch_size int DEFAULT 10000
)
RETURNS bigint
AS $$
DECLARE
    total_rows bigint;
    current_batch int;
    row_count bigint := 0;
    sync_sql text;
BEGIN
    EXECUTE 'SELECT count(*) FROM ' || p_delta_table::text INTO total_rows;

    WHILE total_rows > 0 LOOP
        current_batch := LEAST(p_batch_size, total_rows);

        sync_sql := 'INSERT INTO ' || p_iceberg_oid::text || ' (' || array_to_string(p_col_names, ', ') || ') SELECT ' || array_to_string(p_col_names, ', ') || ' FROM ' || p_delta_table::text || ' ORDER BY ctid LIMIT ' || current_batch;
        EXECUTE sync_sql;

        EXECUTE 'DELETE FROM ' || p_delta_table::text || ' WHERE ctid IN (SELECT ctid FROM ' || p_delta_table::text || ' ORDER BY ctid LIMIT ' || current_batch || ')';

        row_count := row_count + current_batch;
        total_rows := total_rows - current_batch;
    END LOOP;

    RETURN row_count;
END;
$$ LANGUAGE plpgsql;

-- FLUSH 单个 Delta 表到 Iceberg（支持 DML 同步）
CREATE OR REPLACE FUNCTION gaussvector.flush_delta_table(
    delta_table regclass,
    batch_size int DEFAULT 10000
)
RETURNS jsonb
AS $$
DECLARE
    result jsonb;
    iceberg_oid regclass;
    iceberg_location text;
    iceberg_name text;
    delta_schema text;
    delta_name text;
    row_count bigint := 0;
    ddl_count int := 0;
    dml_count int := 0;
    sync_sql text;
    col_sql text;
    col_names text[];
    mor_success boolean := true;
    need_cow_rebuild boolean := false;
BEGIN
    -- 获取 Delta 表信息
    delta_schema := split_part(delta_table::text, '.', 1);
    delta_name := split_part(delta_table::text, '.', 2);

    -- 查询映射
    SELECT iceberg_table_oid, iceberg_location INTO iceberg_oid, iceberg_location
    FROM gaussvector.delta_tables
    WHERE delta_table_oid = delta_table;

    -- ========== 第一步：创建 Iceberg 外表（如果不存在）==========
    IF iceberg_oid IS NULL THEN
        iceberg_name := delta_name || '_iceberg';

        SELECT string_agg(
            quote_ident(attname) || ' ' || format_type(atttypid, atttypmod),
            ', ' ORDER BY attnum
        ) INTO col_sql
        FROM pg_attribute
        WHERE attrelid = delta_table
        AND attnum > 0 AND NOT attisdropped;

        EXECUTE 'CREATE FOREIGN TABLE ' || quote_ident(delta_schema) || '.' || quote_ident(iceberg_name) || ' (' || col_sql || ') SERVER iceberg_server OPTIONS (location ''/data/iceberg/' || delta_schema || '/' || delta_name || ''', table_name ''' || delta_name || ''')';

        iceberg_oid := (delta_schema || '.' || iceberg_name)::regclass;

        UPDATE gaussvector.delta_tables
        SET iceberg_table_oid = iceberg_oid,
            iceberg_location = '/data/iceberg/' || delta_schema || '/' || delta_name
        WHERE delta_table_oid = delta_table;

        RAISE LOG 'FLUSH: Created Iceberg foreign table %s', iceberg_oid::text;
    END IF;

    -- ========== 第二步：同步待处理的 DDL 变化 ==========
    ddl_count := gaussvector.sync_all_ddl_changes(delta_table, iceberg_oid);

    IF ddl_count > 0 THEN
        RAISE LOG 'FLUSH: Synced %s DDL changes to Iceberg', ddl_count;
    END IF;

    -- ========== 新增步骤：同步待处理的 DML 变化（UPDATE/DELETE）==========
    SELECT array_agg(quote_ident(attname) ORDER BY attnum) INTO col_names
    FROM pg_attribute
    WHERE attrelid = delta_table AND attnum > 0 AND NOT attisdropped;

    SELECT count(*) INTO dml_count
    FROM gaussvector.delta_dml_log
    WHERE delta_table_oid = delta_table AND flushed = false;

    IF dml_count > 0 THEN
        DECLARE
            dml_result jsonb;
        BEGIN
            dml_result := gaussvector.sync_all_dml_changes(delta_table, iceberg_oid, col_names, batch_size);
            need_cow_rebuild := (dml_result->>'need_cow_rebuild')::boolean;
            mor_success := (dml_result->>'mor_success')::boolean;
        END;
    END IF;

    -- COW 回退：全表重建
    IF need_cow_rebuild THEN
        PERFORM gaussvector.cow_rebuild_iceberg_table(delta_table, iceberg_oid, batch_size);
        row_count := 0;
    ELSE
        row_count := gaussvector.sync_data_to_iceberg(delta_table, iceberg_oid, col_names, batch_size);
    END IF;

    -- ========== 更新状态 ==========
    UPDATE gaussvector.delta_tables
    SET last_flush_at = current_timestamp,
        pending_changes = false,
        schema_version = schema_version + ddl_count
    WHERE delta_table_oid = delta_table;

    -- 返回结果
    result := jsonb_build_object(
        'delta_table', delta_table::text,
        'iceberg_table', iceberg_oid::text,
        'rows_flushed', row_count,
        'ddl_synced', ddl_count,
        'dml_synced', dml_count,
        'sync_mode', CASE WHEN need_cow_rebuild THEN 'COW' ELSE 'MOR' END,
        'flush_at', current_timestamp
    );

    RAISE LOG 'FLUSH: Completed - %s rows, %s DDL, %s DML changes synced (mode=%s)',
         row_count, ddl_count, dml_count, CASE WHEN need_cow_rebuild THEN 'COW' ELSE 'MOR' END;

    RETURN result;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION gaussvector.flush_delta_table(regclass, int) IS 'Flush delta table data, pending DDL and DML changes to Iceberg (supports MOR and COW fallback)';

-- ==================== 批量 FLUSH ====================

CREATE OR REPLACE FUNCTION gaussvector.flush_all_delta_tables()
RETURNS void
AS $$
DECLARE
    delta_record record;
BEGIN
    FOR delta_record IN
        SELECT delta_table_oid
        FROM gaussvector.delta_tables
        WHERE pending_changes = true
           OR EXISTS (
               SELECT 1 FROM gaussvector.delta_ddl_log
               WHERE delta_table_oid = delta_table_oid AND flushed = false
           )
           OR EXISTS (
               SELECT 1 FROM gaussvector.delta_dml_log
               WHERE delta_table_oid = delta_table_oid AND flushed = false
           )
    LOOP
        PERFORM gaussvector.flush_delta_table(delta_record.delta_table_oid);
    END LOOP;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION gaussvector.flush_all_delta_tables() IS 'Flush all pending delta tables to Iceberg';

-- ==================== 查看状态 ====================

CREATE OR REPLACE FUNCTION gaussvector.show_delta_tables()
RETURNS TABLE (
    delta_table text,
    iceberg_table text,
    pending_changes boolean,
    pending_ddl_count int,
    pending_dml_count int,
    last_flush_at timestamp,
    total_rows bigint
)
AS $$
    SELECT
        dt.delta_table_oid::text,
        dt.iceberg_table_oid::text,
        dt.pending_changes,
        (SELECT count(*) FROM gaussvector.delta_ddl_log ddl
         WHERE ddl.delta_table_oid = dt.delta_table_oid AND ddl.flushed = false)::int,
        (SELECT count(*) FROM gaussvector.delta_dml_log dml
         WHERE dml.delta_table_oid = dt.delta_table_oid AND dml.flushed = false)::int,
        dt.last_flush_at,
        (CASE WHEN dt.delta_table_oid IS NOT NULL THEN
            (SELECT reltuples::bigint FROM pg_class WHERE oid = dt.delta_table_oid)
        ELSE 0 END)::bigint
    FROM gaussvector.delta_tables dt
    ORDER BY dt.delta_created_at;
$$ LANGUAGE sql STABLE;

COMMENT ON FUNCTION gaussvector.show_delta_tables() IS 'Show all delta tables with pending status';

-- ==================== 手动标记变化 ====================

CREATE OR REPLACE FUNCTION gaussvector.mark_delta_pending(delta_table regclass)
RETURNS void
AS $$
BEGIN
    UPDATE gaussvector.delta_tables
    SET pending_changes = true
    WHERE delta_table_oid = delta_table;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Delta table % not found in gaussvector.delta_tables', delta_table::text;
    END IF;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION gaussvector.mark_delta_pending(regclass) IS 'Mark delta table as having pending data to flush';

-- ==================== 清理已同步的 DDL 日志 ====================

CREATE OR REPLACE FUNCTION gaussvector.cleanup_flushed_ddl_log(days_to_keep int DEFAULT 7)
RETURNS int
AS $$
DECLARE
    deleted_count int;
BEGIN
    DELETE FROM gaussvector.delta_ddl_log
    WHERE flushed = true
    AND created_at < current_timestamp - (days_to_keep || ' days')::interval;

    GET DIAGNOSTICS deleted_count = ROW_COUNT;

    RAISE LOG 'Cleaned up %s flushed DDL log entries older than %s days', deleted_count, days_to_keep;

    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION gaussvector.cleanup_flushed_ddl_log(int) IS 'Cleanup old flushed DDL log entries';

-- ==================== DML 同步辅助函数 ====================

-- 获取表的主键列名列表
CREATE OR REPLACE FUNCTION gaussvector.get_table_primary_keys(
    delta_table regclass
)
RETURNS text[]
AS $$
DECLARE
    pk_cols text[];
BEGIN
    -- 使用 pg_constraint + pg_attribute 获取主键列名
    SELECT array_agg(a.attname ORDER BY array_position(c.conkey, a.attnum))
    INTO pk_cols
    FROM pg_constraint c
    JOIN pg_attribute a ON a.attrelid = delta_table AND a.attnum = ANY(c.conkey)
    WHERE c.conrelid = delta_table AND c.contype = 'p';

    IF pk_cols IS NULL OR array_length(pk_cols, 1) IS NULL THEN
        SELECT array_agg(attname ORDER BY attnum)
        INTO pk_cols
        FROM pg_attribute
        WHERE attrelid = delta_table AND attnum > 0 AND NOT attisdropped;
    END IF;

    RETURN pk_cols;
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION gaussvector.get_table_primary_keys(regclass) IS 'Get primary key column names; falls back to all columns if no PK defined';

-- 从 SQL 语句中提取 WHERE 条件
CREATE OR REPLACE FUNCTION gaussvector.extract_where_clause(
    source_sql text
)
RETURNS text
AS $$
DECLARE
    where_clause text;
    lower_sql text;
    lower_where text;
    where_pos int;
    limit_pos int;
    offset_pos int;
    order_pos int;
    group_pos int;
    having_pos int;
    for_pos int;
    end_pos int;
BEGIN
    lower_sql := lower(source_sql);
    where_pos := position('where' in lower_sql);

    IF where_pos = 0 THEN
        RETURN NULL;
    END IF;

    where_clause := substring(source_sql from where_pos + 5);
    lower_where := lower(where_clause);
    end_pos := length(where_clause);

    limit_pos := position('limit' in lower_where);
    IF limit_pos > 0 AND limit_pos < end_pos THEN end_pos := limit_pos; END IF;

    offset_pos := position('offset' in lower_where);
    IF offset_pos > 0 AND offset_pos < end_pos THEN end_pos := offset_pos; END IF;

    order_pos := position('order by' in lower_where);
    IF order_pos > 0 AND order_pos < end_pos THEN end_pos := order_pos; END IF;

    group_pos := position('group by' in lower_where);
    IF group_pos > 0 AND group_pos < end_pos THEN end_pos := group_pos; END IF;

    having_pos := position('having' in lower_where);
    IF having_pos > 0 AND having_pos < end_pos THEN end_pos := having_pos; END IF;

    for_pos := position('for' in lower_where);
    IF for_pos > 0 AND for_pos < end_pos THEN end_pos := for_pos; END IF;

    where_clause := trim(substring(where_clause from 1 for end_pos));
    RETURN where_clause;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION gaussvector.extract_where_clause(text) IS 'Extract WHERE clause from SQL statement';

-- COW 全表重建
CREATE OR REPLACE FUNCTION gaussvector.cow_rebuild_iceberg_table(
    delta_table regclass,
    iceberg_table regclass,
    batch_size int DEFAULT 10000
)
RETURNS void
AS $$
DECLARE
    delta_name text;
    iceberg_name text;
    col_list text;
    all_cols text[];
    total_rows bigint;
    current_batch int;
    sync_sql text;
    delta_schema text;
    col_sql text;
    location text;
    iceberg_base_name text;
BEGIN
    delta_name := delta_table::text;
    iceberg_name := iceberg_table::text;
    delta_schema := split_part(delta_name, '.', 1);
    iceberg_base_name := split_part(iceberg_name, '.', 2);

    SELECT array_agg(quote_ident(attname) ORDER BY attnum) INTO all_cols
    FROM pg_attribute
    WHERE attrelid = delta_table AND attnum > 0 AND NOT attisdropped;

    col_list := array_to_string(all_cols, ', ');

    EXECUTE 'SELECT count(*) FROM ' || delta_name INTO total_rows;

    -- 尝试 DELETE 全表数据
    BEGIN
        EXECUTE 'DELETE FROM ' || iceberg_name;
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'COW: Cannot DELETE from Iceberg FDW, will recreate foreign table';

        EXECUTE 'DROP FOREIGN TABLE ' || iceberg_name;

        SELECT string_agg(
            quote_ident(attname) || ' ' || format_type(atttypid, atttypmod),
            ', ' ORDER BY attnum
        ) INTO col_sql
        FROM pg_attribute
        WHERE attrelid = delta_table AND attnum > 0 AND NOT attisdropped;

        SELECT iceberg_location INTO location
        FROM gaussvector.delta_tables
        WHERE delta_table_oid = delta_table;

        EXECUTE format(
            'CREATE FOREIGN TABLE %I.%I (%s) SERVER iceberg_server
             OPTIONS (location ''%s'', table_name ''%s'')',
            delta_schema, iceberg_base_name, col_sql,
            location, split_part(delta_name, '.', 2)
        );
    END;

    WHILE total_rows > 0 LOOP
        current_batch := LEAST(batch_size, total_rows);

        sync_sql := format(
            'INSERT INTO %s (%s) SELECT %s FROM %s ORDER BY ctid LIMIT %s',
            iceberg_name, col_list, col_list, delta_name, current_batch
        );
        EXECUTE sync_sql;

        total_rows := total_rows - current_batch;
        RAISE LOG 'COW: Batch rebuild - %s rows written to Iceberg', current_batch;
    END LOOP;

    RAISE LOG 'COW: Completed full rebuild of Iceberg table from Delta table';
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION gaussvector.cow_rebuild_iceberg_table(regclass, regclass, int) IS 'Copy-On-Write: full rebuild of Iceberg table from Delta table data';

-- 清理已同步的 DML 日志
CREATE OR REPLACE FUNCTION gaussvector.cleanup_flushed_dml_log(days_to_keep int DEFAULT 7)
RETURNS int
AS $$
DECLARE
    deleted_count int;
BEGIN
    DELETE FROM gaussvector.delta_dml_log
    WHERE flushed = true
    AND created_at < current_timestamp - (days_to_keep || ' days')::interval;

    GET DIAGNOSTICS deleted_count = ROW_COUNT;

    RAISE LOG 'Cleaned up %s flushed DML log entries older than %s days', deleted_count, days_to_keep;

    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION gaussvector.cleanup_flushed_dml_log(int) IS 'Cleanup old flushed DML log entries';

-- ==================== Tuple-Arrow 转化测试函数 ====================

-- 测试：获取 Arrow 类型映射
CREATE OR REPLACE FUNCTION gaussvector.get_arrow_type(pg_type regtype)
RETURNS text
AS $$
DECLARE
    arrow_type text;
BEGIN
    CASE pg_type::oid
        WHEN 16 THEN arrow_type := 'bool';
        WHEN 21 THEN arrow_type := 'int16';
        WHEN 23 THEN arrow_type := 'int32';
        WHEN 20 THEN arrow_type := 'int64';
        WHEN 700 THEN arrow_type := 'float';
        WHEN 701 THEN arrow_type := 'double';
        WHEN 1700 THEN arrow_type := 'decimal';
        WHEN 25 THEN arrow_type := 'string';
        WHEN 1043 THEN arrow_type := 'string';
        WHEN 1082 THEN arrow_type := 'date';
        WHEN 1083 THEN arrow_type := 'time';
        WHEN 1184 THEN arrow_type := 'timestamp_tz';
        WHEN 1114 THEN arrow_type := 'timestamp';
        WHEN 2950 THEN arrow_type := 'uuid';
        WHEN 114 THEN arrow_type := 'json';
        WHEN 3802 THEN arrow_type := 'json';
        WHEN 17 THEN arrow_type := 'binary';
        ELSE arrow_type := 'string';
    END CASE;
    RETURN arrow_type;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION gaussvector.get_arrow_type(regtype) IS 'Get Arrow type name for PostgreSQL type';

-- 测试：检查类型是否支持 Arrow 转化
CREATE OR REPLACE FUNCTION gaussvector.is_arrow_supported(pg_type regtype)
RETURNS boolean
AS $$
BEGIN
    RETURN pg_type::oid IN (16, 21, 23, 20, 700, 701, 1700, 25, 1043, 1082, 1083, 1184, 1114, 2950, 114, 3802, 17);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION gaussvector.is_arrow_supported(regtype) IS 'Check if PostgreSQL type is supported for Arrow conversion';

-- 测试：导出表结构为 Arrow Schema (JSON 格式)
CREATE OR REPLACE FUNCTION gaussvector.table_to_arrow_schema(table_name regclass)
RETURNS jsonb
AS $$
DECLARE
    schema_json jsonb;
BEGIN
    SELECT jsonb_agg(
        jsonb_build_object(
            'name', quote_ident(attname),
            'arrow_type', gaussvector.get_arrow_type(atttypid::regtype),
            'pg_type', format_type(atttypid, atttypmod),
            'nullable', NOT attnotnull
        )
    ) INTO schema_json
    FROM pg_attribute
    WHERE attrelid = table_name
    AND attnum > 0
    AND NOT attisdropped
    ORDER BY attnum;

    RETURN schema_json;
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION gaussvector.table_to_arrow_schema(regclass) IS 'Export table structure as Arrow Schema in JSON format';

-- 测试：批量导出数据到 Parquet（简化版）
CREATE OR REPLACE FUNCTION gaussvector.export_to_parquet(
    table_name regclass,
    file_path text,
    batch_size int DEFAULT 10000
)
RETURNS jsonb
AS $$
DECLARE
    result jsonb;
    row_count bigint;
    schema_json jsonb;
BEGIN
    schema_json := gaussvector.table_to_arrow_schema(table_name);

    EXECUTE 'SELECT count(*) FROM ' || table_name::text INTO row_count;

    result := jsonb_build_object(
        'table', table_name::text,
        'file_path', file_path,
        'schema', schema_json,
        'row_count', row_count,
        'batch_size', batch_size,
        'status', 'schema_ready',
        'note', 'Full Parquet write requires Arrow C++ library'
    );

    RAISE LOG 'Arrow: Schema ready for export %s to %s', table_name::text, file_path;

    RETURN result;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION gaussvector.export_to_parquet(regclass, text, int) IS 'Export table to Parquet file (schema generation only)';

-- 测试：导入 Parquet 数据（简化版）
CREATE OR REPLACE FUNCTION gaussvector.import_from_parquet(
    table_name regclass,
    file_path text,
    batch_size int DEFAULT 10000
)
RETURNS jsonb
AS $$
DECLARE
    result jsonb;
    schema_json jsonb;
BEGIN
    schema_json := gaussvector.table_to_arrow_schema(table_name);

    result := jsonb_build_object(
        'table', table_name::text,
        'file_path', file_path,
        'expected_schema', schema_json,
        'batch_size', batch_size,
        'status', 'schema_ready',
        'note', 'Full Parquet read requires Arrow C++ library'
    );

    RAISE LOG 'Arrow: Schema ready for import from %s to %s', file_path, table_name::text;

    RETURN result;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION gaussvector.import_from_parquet(regclass, text, int) IS 'Import Parquet file to table (schema validation only)';