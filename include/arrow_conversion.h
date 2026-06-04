/*
 * arrow_conversion.h - Tuple <-> Arrow Bidirectional Conversion
 *
 * Provides functions for converting between PostgreSQL HeapTuple
 * and Apache Arrow Table format.
 *
 * Architecture:
 *   HeapTuple → Arrow Table (for Parquet export)
 *   Arrow Table → HeapTuple (for Parquet import)
 */

#ifndef ARROW_CONVERSION_H
#define ARROW_CONVERSION_H

#include "postgres.h"
#include "access/htup.h"
#include "access/tupdesc.h"
#include "executor/tuptable.h"

/* Arrow structures (simplified) */
typedef struct ArrowSchema {
    int n_fields;
    char **field_names;
    int *field_types;
    int *precisions;
    int *scales;
} ArrowSchema;

typedef struct ArrowArray {
    int length;
    int null_count;
    int offset;
    int n_buffers;
    void **buffers;
    int64 *buffer_sizes;
} ArrowArray;

typedef struct ArrowTable {
    ArrowSchema *schema;
    int n_rows;
    ArrowArray **columns;
} ArrowTable;

/* ==================== Conversion Context ==================== */

typedef struct ArrowConversionContext {
    MemoryContext mem_cxt;
    TupleDesc tuple_desc;
    ArrowSchema *arrow_schema;
    int batch_size;
    int current_row;
} ArrowConversionContext;

/* ==================== Core Conversion Functions ==================== */

/* Create conversion context */
extern ArrowConversionContext *CreateArrowConversionContext(TupleDesc desc);

/* Destroy conversion context */
extern void DestroyArrowConversionContext(ArrowConversionContext *ctx);

/* ==================== Tuple → Arrow Conversion ==================== */

/* Convert single HeapTuple to Arrow row */
extern int HeapTupleToArrowRow(HeapTuple tuple, TupleDesc desc,
                                ArrowTable *table, int row_idx);

/* Convert TupleTableSlot to Arrow row */
extern int TupleSlotToArrowRow(TupleTableSlot *slot, ArrowTable *table,
                                int row_idx);

/* Convert batch of HeapTuples to Arrow Table */
extern ArrowTable *HeapTupleBatchToArrowTable(HeapTuple *tuples, int count,
                                               TupleDesc desc);

/* Convert result set to Arrow Table (for query results) */
extern ArrowTable *ResultSetToArrowTable(Portal portal, TupleDesc desc,
                                          int max_rows);

/* ==================== Arrow → Tuple Conversion ==================== */

/* Convert Arrow row to HeapTuple */
extern HeapTuple ArrowRowToHeapTuple(ArrowTable *table, int row_idx,
                                      TupleDesc desc);

/* Convert Arrow Table to batch of HeapTuples */
extern int ArrowTableToHeapTupleBatch(ArrowTable *table, HeapTuple **tuples,
                                       TupleDesc desc);

/* ==================== Schema Conversion ==================== */

/* Convert TupleDesc to Arrow Schema */
extern ArrowSchema *TupleDescToArrowSchema(TupleDesc desc);

/* Convert Arrow Schema to TupleDesc */
extern TupleDesc ArrowSchemaToTupleDesc(ArrowSchema *schema);

/* ==================== Data Extraction Helpers ==================== */

/* Extract Datum from Arrow array at index */
extern Datum ArrowArrayGetDatum(ArrowArray *array, int idx,
                                 int arrow_type, Oid pg_type);

/* Set Datum to Arrow array at index */
extern void ArrowArraySetDatum(ArrowArray *array, int idx, Datum value,
                                bool is_null, int arrow_type);

/* ==================== Parquet File I/O ==================== */

/* Write Arrow Table to Parquet file */
extern int ArrowTableToParquet(ArrowTable *table, const char *file_path);

/* Read Parquet file to Arrow Table */
extern ArrowTable *ParquetToArrowTable(const char *file_path,
                                        ArrowSchema *expected_schema);

/* Write HeapTuples directly to Parquet */
extern int HeapTuplesToParquet(HeapTuple *tuples, int count, TupleDesc desc,
                                const char *file_path);

/* Read Parquet and insert into relation */
extern int ParquetToRelation(const char *file_path, Relation rel);

/* ==================== Utility Functions ==================== */

/* Get Arrow Table row count */
extern int ArrowTableGetRowCount(ArrowTable *table);

/* Get Arrow Table column count */
extern int ArrowTableGetColumnCount(ArrowTable *table);

/* Check Arrow Table is valid */
extern bool ArrowTableIsValid(ArrowTable *table);

/* Print Arrow Table structure (for debugging) */
extern void ArrowTablePrint(ArrowTable *table);

/* ==================== Configuration ==================== */

#define ARROW_DEFAULT_BATCH_SIZE 2048
#define ARROW_MAX_BATCH_SIZE 10000

#endif /* ARROW_CONVERSION_H */