-- dual_table--1.0.sql - Delta/Iceberg 双表架构（延迟同步模式）
--
-- 架构说明：
-- - Delta 表（ustore 内表）：接收所有 INSERT 和 DDL 操作
-- - Iceberg 表（外表）：存储最终数据（Parquet 格式）
-- - FLUSH 命令：用户主动触发，批量同步数据和结构

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

-- ==================== Iceberg 外部服务器 ====================

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_foreign_data_wrapper WHERE fdwname = 'pg_lake_iceberg') THEN
        CREATE SERVER IF NOT EXISTS iceberg_server FOREIGN DATA WRAPPER pg_lake_iceberg;
    ELSIF EXISTS (SELECT 1 FROM pg_foreign_data_wrapper WHERE fdwname = 'iceberg_fdw') THEN
        CREATE SERVER IF NOT EXISTS iceberg_server FOREIGN DATA WRAPPER iceberg_fdw;
    ELSE
        CREATE SERVER IF NOT EXISTS iceberg_server FOREIGN DATA WRAPPER file_fdw;
        elog(WARNING, 'No Iceberg FDW found, using file_fdw (limited functionality)');
    END IF;
END
$$;

-- ==================== 核心函数：FLUSH ====================

-- FLUSH 单个 Delta 表到 Iceberg
CREATE OR REPLACE FUNCTION gaussvector.flush_delta_table(
    delta_table regclass,
    batch_size int DEFAULT 10000
)
RETURNS jsonb
AS $$
DECLARE
    result jsonb;
    iceberg_oid regclass;
    iceberg_name text;
    delta_schema text;
    delta_name text;
    row_count bigint;
    ddl_count int;
    sync_sql text;
    col_sql text;
    col_names text[];
    ddl_record record;
    -- 分批同步相关变量
    total_rows bigint;
    current_batch int;
    batch_num int;
BEGIN
    -- 获取 Delta 表信息
    delta_schema := split_part(delta_table::text, '.', 1);
    delta_name := split_part(delta_table::text, '.', 2);

    -- 查询映射
    SELECT iceberg_table_oid, iceberg_location INTO iceberg_oid, result
    FROM gaussvector.delta_tables
    WHERE delta_table_oid = delta_table;

    -- ========== 第一步：创建 Iceberg 外表（如果不存在）==========
    IF iceberg_oid IS NULL THEN
        -- 外表不存在，需要创建
        iceberg_name := delta_name || '_iceberg';

        -- 构建列定义
        SELECT string_agg(
            quote_ident(attname) || ' ' || format_type(atttypid, atttypmod),
            ', ' ORDER BY attnum
        ) INTO col_sql
        FROM pg_attribute
        WHERE attrelid = delta_table
        AND attnum > 0 AND NOT attisdropped;

        -- 创建外表
        EXECUTE format(
            'CREATE FOREIGN TABLE %I.%I (%s) SERVER iceberg_server
             OPTIONS (location ''/data/iceberg/%s/%s'', table_name ''%s'')',
            delta_schema, iceberg_name, col_sql,
            delta_schema, delta_name, delta_name
        );

        -- 获取新创建的外表 OID
        iceberg_oid := (delta_schema || '.' || iceberg_name)::regclass;

        -- 更新映射
        UPDATE gaussvector.delta_tables
        SET iceberg_table_oid = iceberg_oid,
            iceberg_location = '/data/iceberg/' || delta_schema || '/' || delta_name
        WHERE delta_table_oid = delta_table;

        elog(LOG, 'FLUSH: Created Iceberg foreign table %s', iceberg_oid::text);
    END IF;

    -- ========== 第二步：同步待处理的 DDL 变化 ==========
    SELECT count(*) INTO ddl_count
    FROM gaussvector.delta_ddl_log
    WHERE delta_table_oid = delta_table AND flushed = false;

    IF ddl_count > 0 THEN
        FOR ddl_record IN
            SELECT id, ddl_type, ddl_detail
            FROM gaussvector.delta_ddl_log
            WHERE delta_table_oid = delta_table AND flushed = false
            ORDER BY created_at
        DO
            -- 根据 DDL 类型同步
            CASE ddl_record.ddl_type
                WHEN 'ADD_COLUMN' THEN
                    EXECUTE format(
                        'ALTER FOREIGN TABLE %s ADD COLUMN %I %s',
                        iceberg_oid::text,
                        ddl_record.ddl_detail->>'column_name',
                        ddl_record.ddl_detail->>'column_type'
                    );
                WHEN 'DROP_COLUMN' THEN
                    EXECUTE format(
                        'ALTER FOREIGN TABLE %s DROP COLUMN %I',
                        iceberg_oid::text,
                        ddl_record.ddl_detail->>'column_name'
                    );
                WHEN 'ALTER_TYPE' THEN
                    EXECUTE format(
                        'ALTER FOREIGN TABLE %s ALTER COLUMN %I TYPE %s',
                        iceberg_oid::text,
                        ddl_record.ddl_detail->>'column_name',
                        ddl_record.ddl_detail->>'new_type'
                    );
                WHEN 'SET_NOT_NULL' THEN
                    EXECUTE format(
                        'ALTER FOREIGN TABLE %s ALTER COLUMN %I SET NOT NULL',
                        iceberg_oid::text,
                        ddl_record.ddl_detail->>'column_name'
                    );
                WHEN 'DROP_NOT_NULL' THEN
                    EXECUTE format(
                        'ALTER FOREIGN TABLE %s ALTER COLUMN %I DROP NOT NULL',
                        iceberg_oid::text,
                        ddl_record.ddl_detail->>'column_name'
                    );
                ELSE
                    elog(WARNING, 'FLUSH: Unknown DDL type %s', ddl_record.ddl_type);
            END CASE;

            -- 标记 DDL 已同步
            UPDATE gaussvector.delta_ddl_log
            SET flushed = true
            WHERE id = ddl_record.id;
        END LOOP;

        elog(LOG, 'FLUSH: Synced %d DDL changes to Iceberg', ddl_count);
    END IF;

    -- ========== 第三步：分批同步数据到 Iceberg ==========
    -- 获取列名列表
    SELECT array_agg(quote_ident(attname) ORDER BY attnum) INTO col_names
    FROM pg_attribute
    WHERE attrelid = delta_table
    AND attnum > 0 AND NOT attisdropped;

    -- 获取 Delta 表总行数
    EXECUTE format('SELECT count(*) FROM %s', delta_table::text);
    GET DIAGNOSTICS total_rows = ROW_COUNT;

    row_count := 0;
    batch_num := 0;

    -- 分批写入 Iceberg
    WHILE total_rows > 0 LOOP
        -- 计算当前批次行数
        current_batch := LEAST(batch_size, total_rows);

        -- 分批插入到 Iceberg
        sync_sql := format(
            'INSERT INTO %s (%s) SELECT %s FROM %s ORDER BY ctid LIMIT %s',
            iceberg_oid::text,
            array_to_string(col_names, ', '),
            array_to_string(col_names, ', '),
            delta_table::text,
            current_batch
        );

        EXECUTE sync_sql;

        -- 删除已同步的 Delta 数据（避免重复同步）
        EXECUTE format(
            'DELETE FROM %s WHERE ctid IN (
                SELECT ctid FROM %s ORDER BY ctid LIMIT %s
            )',
            delta_table::text,
            delta_table::text,
            current_batch
        );

        row_count := row_count + current_batch;
        total_rows := total_rows - current_batch;
        batch_num := batch_num + 1;

        elog(LOG, 'FLUSH: Batch %d - %d rows synced to Iceberg', batch_num, current_batch);
    END LOOP;

    -- ========== 第四步：更新状态 ==========
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
        'flush_at', current_timestamp
    );

    elog(LOG, 'FLUSH: Completed - %d rows, %d DDL changes synced', row_count, ddl_count);

    RETURN result;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION gaussvector.flush_delta_table(regclass, int) IS 'Flush delta table data and pending DDL to Iceberg';

-- ==================== 批量 FLUSH ====================

-- FLUSH 所有待处理的 Delta 表
CREATE OR REPLACE FUNCTION gaussvector.flush_all_delta_tables()
RETURNS TABLE (
    delta_table text,
    iceberg_table text,
    rows_flushed bigint,
    ddl_synced int
)
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
    DO
        -- 执行单个表的 FLUSH
        PERFORM gaussvector.flush_delta_table(delta_record.delta_table_oid);

        -- 返回结果
        RETURN QUERY SELECT
            delta_record.delta_table_oid::text,
            dt.iceberg_table_oid::text,
            (SELECT count(*) FROM gaussvector.delta_ddl_log
             WHERE delta_table_oid = delta_record.delta_table_oid AND flushed = true)::bigint,
            (SELECT count(*) FROM gaussvector.delta_ddl_log
             WHERE delta_table_oid = delta_record.delta_table_oid)::int
        FROM gaussvector.delta_tables dt
        WHERE dt.delta_table_oid = delta_record.delta_table_oid;
    END LOOP;

    RETURN;
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
    last_flush_at timestamp,
    total_rows bigint
)
AS $$
    RETURN QUERY SELECT
        dt.delta_table_oid::text,
        dt.iceberg_table_oid::text,
        dt.pending_changes,
        (SELECT count(*) FROM gaussvector.delta_ddl_log ddl
         WHERE ddl.delta_table_oid = dt.delta_table_oid AND ddl.flushed = false)::int,
        dt.last_flush_at,
        (CASE WHEN dt.delta_table_oid IS NOT NULL THEN
            (SELECT reltuples::bigint FROM pg_class WHERE oid = dt.delta_table_oid)
        ELSE 0 END)::bigint
    FROM gaussvector.delta_tables dt
    ORDER BY dt.delta_created_at;
$$ LANGUAGE sql STABLE;

COMMENT ON FUNCTION gaussvector.show_delta_tables() IS 'Show all delta tables with pending status';

-- ==================== 手动标记变化 ====================

-- 手动标记 Delta 表有待刷新的数据（用于外部程序调用）
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

    elog(LOG, 'Cleaned up %d flushed DDL log entries older than %d days', deleted_count, days_to_keep);

    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION gaussvector.cleanup_flushed_ddl_log(int) IS 'Cleanup old flushed DDL log entries';

-- ==================== Tuple-Arrow 转化测试函数 ====================

-- 测试：获取 Arrow 类型映射
CREATE OR REPLACE FUNCTION gaussvector.get_arrow_type(pg_type regtype)
RETURNS text
AS $$
DECLARE
    arrow_type text;
BEGIN
    -- 调用 C 函数获取 Arrow 类型名称
    -- 目前返回基于类型 OID 的字符串映射
    CASE pg_type::oid
        WHEN 16 THEN arrow_type := 'bool';         -- BOOLOID
        WHEN 21 THEN arrow_type := 'int16';        -- INT2OID
        WHEN 23 THEN arrow_type := 'int32';        -- INT4OID
        WHEN 20 THEN arrow_type := 'int64';        -- INT8OID
        WHEN 700 THEN arrow_type := 'float';       -- FLOAT4OID
        WHEN 701 THEN arrow_type := 'double';      -- FLOAT8OID
        WHEN 1700 THEN arrow_type := 'decimal';    -- NUMERICOID
        WHEN 25 THEN arrow_type := 'string';       -- TEXTOID
        WHEN 1043 THEN arrow_type := 'string';     -- VARCHAROID
        WHEN 1082 THEN arrow_type := 'date';       -- DATEOID
        WHEN 1083 THEN arrow_type := 'time';       -- TIMEOID
        WHEN 1184 THEN arrow_type := 'timestamp_tz'; -- TIMESTAMPTZOID
        WHEN 1114 THEN arrow_type := 'timestamp';  -- TIMESTAMPOID
        WHEN 2950 THEN arrow_type := 'uuid';       -- UUIDOID
        WHEN 114 THEN arrow_type := 'json';        -- JSONOID
        WHEN 3802 THEN arrow_type := 'json';       -- JSONBOID
        WHEN 17 THEN arrow_type := 'binary';       -- BYTEAOID
        ELSE arrow_type := 'string';               -- 默认映射为 string
    END CASE;

    RETURN arrow_type;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION gaussvector.get_arrow_type(regtype) IS 'Get Arrow type name for PostgreSQL type';

-- 测试：检查类型是否支持 Arrow 转化
CREATE OR REPLACE FUNCTION gaussvector.is_arrow_supported(pg_type regtype)
RETURNS boolean
AS $$
DECLARE
    supported boolean;
BEGIN
    -- 检查是否在支持列表中
    supported := pg_type::oid IN (
        16,    -- bool
        21,    -- int2
        23,    -- int4
        20,    -- int8
        700,   -- float4
        701,   -- float8
        1700,  -- numeric
        25,    -- text
        1043,  -- varchar
        1082,  -- date
        1083,  -- time
        1184,  -- timestamptz
        1114,  -- timestamp
        2950,  -- uuid
        114,   -- json
        3802,  -- jsonb
        17     -- bytea
    );

    RETURN supported;
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
    -- 构建 Arrow Schema JSON
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
    -- 获取表结构
    schema_json := gaussvector.table_to_arrow_schema(table_name);

    -- 获取行数
    EXECUTE format('SELECT count(*) FROM %s', table_name::text);
    GET DIAGNOSTICS row_count = ROW_COUNT;

    -- 记录导出信息（实际 Parquet 写入需要 Arrow C++ 库）
    result := jsonb_build_object(
        'table', table_name::text,
        'file_path', file_path,
        'schema', schema_json,
        'row_count', row_count,
        'batch_size', batch_size,
        'status', 'schema_ready',
        'note', 'Full Parquet write requires Arrow C++ library'
    );

    elog(LOG, 'Arrow: Schema ready for export %s to %s', table_name::text, file_path);

    RETURN result;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION gaussvector.export_to_parquet(regclass, text, int) IS 'Export table to Parquet file (schema generation only, full write requires Arrow C++)';

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
    -- 获取目标表结构
    schema_json := gaussvector.table_to_arrow_schema(table_name);

    -- 记录导入信息（实际 Parquet 读取需要 Arrow C++ 库）
    result := jsonb_build_object(
        'table', table_name::text,
        'file_path', file_path,
        'expected_schema', schema_json,
        'batch_size', batch_size,
        'status', 'schema_ready',
        'note', 'Full Parquet read requires Arrow C++ library'
    );

    elog(LOG, 'Arrow: Schema ready for import from %s to %s', file_path, table_name::text);

    RETURN result;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION gaussvector.import_from_parquet(regclass, text, int) IS 'Import Parquet file to table (schema validation only, full read requires Arrow C++)';