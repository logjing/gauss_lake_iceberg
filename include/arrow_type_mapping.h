/*
 * arrow_type_mapping.h - PostgreSQL <-> Arrow Type Mapping
 *
 * Defines type mapping between PostgreSQL types and Arrow types
 * for Tuple-Arrow conversion.
 */

#ifndef ARROW_TYPE_MAPPING_H
#define ARROW_TYPE_MAPPING_H

#include "postgres.h"
#include "catalog/pg_type.h"

/* ==================== Arrow Type IDs ==================== */

/* Basic types */
#define ARROW_TYPE_BOOL         1
#define ARROW_TYPE_INT8         2
#define ARROW_TYPE_INT16        3
#define ARROW_TYPE_INT32        4
#define ARROW_TYPE_INT64        5
#define ARROW_TYPE_UINT8        6
#define ARROW_TYPE_UINT16       7
#define ARROW_TYPE_UINT32       8
#define ARROW_TYPE_UINT64       9
#define ARROW_TYPE_FLOAT        10
#define ARROW_TYPE_DOUBLE       11
#define ARROW_TYPE_STRING       12
#define ARROW_TYPE_BINARY       13
#define ARROW_TYPE_DATE         14
#define ARROW_TYPE_TIMESTAMP    15
#define ARROW_TYPE_TIME         16
#define ARROW_TYPE_DECIMAL      17
#define ARROW_TYPE_LIST         18
#define ARROW_TYPE_STRUCT       19
#define ARROW_TYPE_MAP          20
#define ARROW_TYPE_TIMESTAMP_TZ 21
#define ARROW_TYPE_TIME_TZ      22
#define ARROW_TYPE_INTERVAL     23
#define ARROW_TYPE_UUID         24
#define ARROW_TYPE_JSON         25
#define ARROW_TYPE_GEOMETRY     26

/* ==================== Type Mapping Entry ==================== */

typedef struct ArrowTypeMapping {
    Oid pg_type_oid;        /* PostgreSQL type OID */
    int arrow_type_id;      /* Arrow type ID */
    int arrow_precision;    /* For decimal: precision */
    int arrow_scale;        /* For decimal: scale */
    int arrow_bit_width;    /* For int types: bit width */
    const char *arrow_name; /* Arrow type name */
} ArrowTypeMapping;

/* ==================== Function Declarations ==================== */

/* Get Arrow type ID from PostgreSQL type OID */
extern int GetArrowTypeId(Oid pg_type_oid);

/* Get Arrow type mapping entry */
extern ArrowTypeMapping *GetArrowTypeMapping(Oid pg_type_oid);

/* Get PostgreSQL type OID from Arrow type ID */
extern Oid GetPgTypeOid(int arrow_type_id, int precision, int scale);

/* Check if type is supported for Arrow conversion */
extern bool IsArrowTypeSupported(Oid pg_type_oid);

/* Get Arrow type name */
extern const char *GetArrowTypeName(int arrow_type_id);

/* Get all supported PostgreSQL types */
extern Oid *GetSupportedPgTypes(int *count);

/* ==================== Helper Macros ==================== */

#define ARROW_TYPE_IS_INT(type_id) \
    ((type_id) >= ARROW_TYPE_INT8 && (type_id) <= ARROW_TYPE_UINT64)

#define ARROW_TYPE_IS_FLOAT(type_id) \
    ((type_id) == ARROW_TYPE_FLOAT || (type_id) == ARROW_TYPE_DOUBLE)

#define ARROW_TYPE_IS_NUMERIC(type_id) \
    (ARROW_TYPE_IS_INT(type_id) || ARROW_TYPE_IS_FLOAT(type_id) || \
     (type_id) == ARROW_TYPE_DECIMAL)

#define ARROW_TYPE_IS_TEMPORAL(type_id) \
    ((type_id) >= ARROW_TYPE_DATE && (type_id) <= ARROW_TYPE_TIME_TZ)

#endif /* ARROW_TYPE_MAPPING_H */