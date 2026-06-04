/*
 * arrow_type_mapping.c - PostgreSQL <-> Arrow Type Mapping Implementation
 *
 * Implements type mapping between PostgreSQL types and Arrow types.
 */

#include "../include/arrow_type_mapping.h"
#include "utils/builtins.h"
#include "catalog/pg_type.h"

/* ==================== Type Mapping Table ==================== */

static ArrowTypeMapping typeMappings[] = {
    /* Boolean */
    {BOOLOID, ARROW_TYPE_BOOL, 0, 0, 1, "bool"},

    /* Integer types */
    {INT1OID, ARROW_TYPE_INT8, 0, 0, 8, "int8"},
    {INT2OID, ARROW_TYPE_INT16, 0, 0, 16, "int16"},
    {INT4OID, ARROW_TYPE_INT32, 0, 0, 32, "int32"},
    {INT8OID, ARROW_TYPE_INT64, 0, 0, 64, "int64"},

    /* Unsigned integer types (if supported) */
    {UINT1OID, ARROW_TYPE_UINT8, 0, 0, 8, "uint8"},
    {UINT2OID, ARROW_TYPE_UINT16, 0, 0, 16, "uint16"},
    {UINT4OID, ARROW_TYPE_UINT32, 0, 0, 32, "uint32"},
    {UINT8OID, ARROW_TYPE_UINT64, 0, 0, 64, "uint64"},

    /* Floating point types */
    {FLOAT4OID, ARROW_TYPE_FLOAT, 0, 0, 32, "float"},
    {FLOAT8OID, ARROW_TYPE_DOUBLE, 0, 0, 64, "double"},

    /* Numeric/Decimal */
    {NUMERICOID, ARROW_TYPE_DECIMAL, 38, 0, 0, "decimal"},

    /* Character types */
    {CHAROID, ARROW_TYPE_STRING, 0, 0, 0, "string"},
    {BPCHAROID, ARROW_TYPE_STRING, 0, 0, 0, "string"},
    {VARCHAROID, ARROW_TYPE_STRING, 0, 0, 0, "string"},
    {TEXTOID, ARROW_TYPE_STRING, 0, 0, 0, "string"},
    {CSTRINGOID, ARROW_TYPE_STRING, 0, 0, 0, "string"},

    /* Binary types */
    {BYTEAOID, ARROW_TYPE_BINARY, 0, 0, 0, "binary"},
    {VARBITOID, ARROW_TYPE_BINARY, 0, 0, 0, "binary"},
    {BITOID, ARROW_TYPE_BINARY, 0, 0, 0, "binary"},

    /* Date/Time types */
    {DATEOID, ARROW_TYPE_DATE, 0, 0, 0, "date"},
    {TIMEOID, ARROW_TYPE_TIME, 0, 0, 0, "time"},
    {TIMETZOID, ARROW_TYPE_TIME_TZ, 0, 0, 0, "time_tz"},
    {TIMESTAMPOID, ARROW_TYPE_TIMESTAMP, 0, 0, 0, "timestamp"},
    {TIMESTAMPTZOID, ARROW_TYPE_TIMESTAMP_TZ, 0, 0, 0, "timestamp_tz"},
    {INTERVALOID, ARROW_TYPE_INTERVAL, 0, 0, 0, "interval"},

    /* UUID */
    {UUIDOID, ARROW_TYPE_UUID, 0, 0, 0, "uuid"},

    /* JSON */
    {JSONOID, ARROW_TYPE_JSON, 0, 0, 0, "json"},
    {JSONBOID, ARROW_TYPE_JSON, 0, 0, 0, "json"},

    /* Array/List type */
    {ANYARRAYOID, ARROW_TYPE_LIST, 0, 0, 0, "list"},

    /* Composite/Struct type */
    {RECORDOID, ARROW_TYPE_STRUCT, 0, 0, 0, "struct"},

    /* Geometry (if PostGIS available) */
    /* {GEOMETRYOID, ARROW_TYPE_GEOMETRY, 0, 0, 0, "geometry"}, */
};

#define NUM_TYPE_MAPPINGS (sizeof(typeMappings) / sizeof(ArrowTypeMapping))

/* ==================== Implementation ==================== */

/*
 * GetArrowTypeId - Get Arrow type ID from PostgreSQL type OID
 */
int
GetArrowTypeId(Oid pg_type_oid)
{
    int arrow_type_id;

    /* First try exact match */
    for (int i = 0; i < NUM_TYPE_MAPPINGS; i++)
    {
        if (typeMappings[i].pg_type_oid == pg_type_oid)
        {
            return typeMappings[i].arrow_type_id;
        }
    }

    /* Handle array types */
    if (OidIsValid(get_element_type(pg_type_oid)))
    {
        return ARROW_TYPE_LIST;
    }

    /* Handle composite types */
    if (type_is_composite(pg_type_oid))
    {
        return ARROW_TYPE_STRUCT;
    }

    /* Unknown type */
    elog(WARNING, "Arrow: Unknown PostgreSQL type OID %u, mapping to string", pg_type_oid);
    return ARROW_TYPE_STRING;
}

/*
 * GetArrowTypeMapping - Get full mapping entry for PostgreSQL type
 */
ArrowTypeMapping *
GetArrowTypeMapping(Oid pg_type_oid)
{
    for (int i = 0; i < NUM_TYPE_MAPPINGS; i++)
    {
        if (typeMappings[i].pg_type_oid == pg_type_oid)
        {
            return &typeMappings[i];
        }
    }
    return NULL;
}

/*
 * GetPgTypeOid - Get PostgreSQL type OID from Arrow type
 */
Oid
GetPgTypeOid(int arrow_type_id, int precision, int scale)
{
    switch (arrow_type_id)
    {
        case ARROW_TYPE_BOOL:
            return BOOLOID;
        case ARROW_TYPE_INT8:
            return INT1OID;
        case ARROW_TYPE_INT16:
            return INT2OID;
        case ARROW_TYPE_INT32:
            return INT4OID;
        case ARROW_TYPE_INT64:
            return INT8OID;
        case ARROW_TYPE_UINT8:
            return UINT1OID;
        case ARROW_TYPE_UINT16:
            return UINT2OID;
        case ARROW_TYPE_UINT32:
            return UINT4OID;
        case ARROW_TYPE_UINT64:
            return UINT8OID;
        case ARROW_TYPE_FLOAT:
            return FLOAT4OID;
        case ARROW_TYPE_DOUBLE:
            return FLOAT8OID;
        case ARROW_TYPE_DECIMAL:
            return NUMERICOID;
        case ARROW_TYPE_STRING:
            return TEXTOID;
        case ARROW_TYPE_BINARY:
            return BYTEAOID;
        case ARROW_TYPE_DATE:
            return DATEOID;
        case ARROW_TYPE_TIME:
            return TIMEOID;
        case ARROW_TYPE_TIME_TZ:
            return TIMETZOID;
        case ARROW_TYPE_TIMESTAMP:
            return TIMESTAMPOID;
        case ARROW_TYPE_TIMESTAMP_TZ:
            return TIMESTAMPTZOID;
        case ARROW_TYPE_INTERVAL:
            return INTERVALOID;
        case ARROW_TYPE_UUID:
            return UUIDOID;
        case ARROW_TYPE_JSON:
            return JSONOID;
        case ARROW_TYPE_LIST:
            return ANYARRAYOID;
        case ARROW_TYPE_STRUCT:
            return RECORDOID;
        default:
            elog(WARNING, "Arrow: Unknown Arrow type %d, mapping to text", arrow_type_id);
            return TEXTOID;
    }
}

/*
 * IsArrowTypeSupported - Check if PostgreSQL type can be converted to Arrow
 */
bool
IsArrowTypeSupported(Oid pg_type_oid)
{
    /* Check if we have a direct mapping */
    if (GetArrowTypeMapping(pg_type_oid) != NULL)
        return true;

    /* Arrays are supported if element type is supported */
    Oid element_type = get_element_type(pg_type_oid);
    if (OidIsValid(element_type))
        return IsArrowTypeSupported(element_type);

    /* Composite types are supported if all fields are supported */
    if (type_is_composite(pg_type_oid))
        return true;  /* Will validate field types during conversion */

    return false;
}

/*
 * GetArrowTypeName - Get human-readable Arrow type name
 */
const char *
GetArrowTypeName(int arrow_type_id)
{
    for (int i = 0; i < NUM_TYPE_MAPPINGS; i++)
    {
        if (typeMappings[i].arrow_type_id == arrow_type_id)
        {
            return typeMappings[i].arrow_name;
        }
    }
    return "unknown";
}

/*
 * GetSupportedPgTypes - Get list of all supported PostgreSQL types
 */
Oid *
GetSupportedPgTypes(int *count)
{
    Oid *types = (Oid *) palloc(NUM_TYPE_MAPPINGS * sizeof(Oid));

    for (int i = 0; i < NUM_TYPE_MAPPINGS; i++)
    {
        types[i] = typeMappings[i].pg_type_oid;
    }

    *count = NUM_TYPE_MAPPINGS;
    return types;
}