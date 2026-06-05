/*
 * delta_copy_from.cpp - COPY FROM Delta table batch import implementation
 *
 * Features:
 * - Intercept COPY FROM command, identify Delta table target
 * - Use openGauss native batch insert mechanism (heap_multi_insert)
 * - Mark pending_changes after import completion
 *
 * openGauss BeginCopyFrom API:
 *   CopyState BeginCopyFrom(Relation rel, const char* filename, List* attnamelist,
 *       List* options, void* mem_info, const char* queryString, CopyGetDataFunc func)
 */

#include "../include/dual_table.h"

/* COPY related headers */
#include "commands/copy.h"

/* Permission related */
#include "utils/acl.h"
#include "catalog/pg_class.h"

/* ==================== COPY FROM Batch Import Handler ==================== */

/*
 * ProcessDeltaTableCopyFrom - Handle COPY FROM to Delta table
 *
 * Core flow:
 * 1. Permission check
 * 2. BeginCopyFrom init (auto-enable batch insert judgment)
 * 3. CopyFrom execute (internally auto-enable useHeapMultiInsert)
 * 4. Mark pending_changes
 * 5. EndCopyFrom cleanup
 */
void
ProcessDeltaTableCopyFrom(CopyStmt *stmt, Relation rel,
                          ParseState *pstate, char *completionTag)
{
    Oid relid = RelationGetRelid(rel);
    uint64 rowsProcessed = 0;
    CopyState cstate = NULL;

    elog(LOG, "Delta Table: Processing COPY FROM for delta table %u", relid);

    /* 1. Permission check */
    AclResult aclresult = pg_class_aclcheck(relid, GetUserId(), ACL_INSERT);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, ACL_KIND_CLASS, RelationGetRelationName(rel));

    PG_TRY();
    {
        /*
         * BeginCopyFrom initializes COPY state (openGauss API)
         * Note: CopyFrom internally determines whether to enable batch insert (useHeapMultiInsert)
         * Delta internal table satisfies batch insert conditions:
         * - No BEFORE/INSTEAD OF INSERT triggers
         * - Default value expressions are non-volatile
         * - Not a foreign table
         */
        cstate = BeginCopyFrom(
            rel,
            stmt->filename,
            stmt->attlist,      /* Column list, can be NULL */
            stmt->options,
            NULL,               /* mem_info */
            pstate->p_sourcetext,  /* queryString */
            NULL);              /* data_source_cb */

        /* CopyFrom executes data import */
        rowsProcessed = CopyFrom(cstate);

        /* Mark pending_changes */
        MarkDeltaTablePendingChanges(relid);

        /* Cleanup COPY state */
        EndCopyFrom(cstate);
        cstate = NULL;
    }
    PG_CATCH();
    {
        /* Ensure cleanup on exception */
        if (cstate)
            EndCopyFrom(cstate);

        PG_RE_THROW();
    }
    PG_END_TRY();

    /* Set completion tag */
    if (completionTag)
    {
        snprintf(completionTag, COMPLETION_TAG_BUFSIZE,
                 "COPY " UINT64_FORMAT, rowsProcessed);
    }

    elog(LOG, "Delta Table: COPY FROM imported " UINT64_FORMAT " rows to delta table %u",
         rowsProcessed, relid);
}

/* ==================== Mark Pending Data for Refresh ==================== */

/*
 * MarkDeltaTablePendingChanges - Mark Delta table has data pending refresh
 *
 * Update gaussvector.delta_tables pending_changes to true
 */
void
MarkDeltaTablePendingChanges(Oid delta_relid)
{
    SPI_CONNECT_COMPAT();

    StringInfoData query;
    initStringInfo(&query);

    appendStringInfo(&query,
        "UPDATE gaussvector.delta_tables "
        "SET pending_changes = true "
        "WHERE delta_table_oid = %u",
        delta_relid);

    SPI_EXECUTE_COMPAT(query.data, false, 0);
    SPI_FINISH_COMPAT();

    elog(DEBUG1, "Delta Table: Marked pending_changes for delta table %u", delta_relid);

    pfree(query.data);
}