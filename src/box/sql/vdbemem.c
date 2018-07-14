/*
 * Copyright 2010-2017, Tarantool AUTHORS, please see AUTHORS file.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

/*
 *
 * This file contains code use to manipulate "Mem" structure.  A "Mem"
 * stores a single value in the VDBE.  Mem is an opaque structure visible
 * only within the VDBE.  Interface routines refer to a Mem using the
 * name sqlite_value
 */
#include "sqliteInt.h"
#include "vdbeInt.h"
#include "tarantoolInt.h"

#ifdef SQLITE_DEBUG
/*
 * Check invariants on a Mem object.
 *
 * This routine is intended for use inside of assert() statements, like
 * this:    assert( sqlite3VdbeCheckMemInvariants(pMem) );
 */
int
sqlite3VdbeCheckMemInvariants(Mem * p)
{
	/* If MEM_Dyn is set then Mem.xDel!=0.
	 * Mem.xDel is might not be initialized if MEM_Dyn is clear.
	 */
	assert((p->flags & MEM_Dyn) == 0 || p->xDel != 0);

	/* MEM_Dyn may only be set if Mem.szMalloc==0.  In this way we
	 * ensure that if Mem.szMalloc>0 then it is safe to do
	 * Mem.z = Mem.zMalloc without having to check Mem.flags&MEM_Dyn.
	 * That saves a few cycles in inner loops.
	 */
	assert((p->flags & MEM_Dyn) == 0 || p->szMalloc == 0);

	/* Cannot be both MEM_Int and MEM_Real at the same time */
	assert((p->flags & (MEM_Int | MEM_Real)) != (MEM_Int | MEM_Real));

	/* The szMalloc field holds the correct memory allocation size */
	assert(p->szMalloc == 0
	       || p->szMalloc == sqlite3DbMallocSize(p->db, p->zMalloc));

	/* If p holds a string or blob, the Mem.z must point to exactly
	 * one of the following:
	 *
	 *   (1) Memory in Mem.zMalloc and managed by the Mem object
	 *   (2) Memory to be freed using Mem.xDel
	 *   (3) An ephemeral string or blob
	 *   (4) A static string or blob
	 */
	if ((p->flags & (MEM_Str | MEM_Blob)) && p->n > 0) {
		assert(((p->szMalloc > 0 && p->z == p->zMalloc) ? 1 : 0) +
		       ((p->flags & MEM_Dyn) != 0 ? 1 : 0) +
		       ((p->flags & MEM_Ephem) != 0 ? 1 : 0) +
		       ((p->flags & MEM_Static) != 0 ? 1 : 0) == 1);
	}
	return 1;
}
#endif

/*
 * Make sure pMem->z points to a writable allocation of at least
 * min(n,32) bytes.
 *
 * If the bPreserve argument is true, then copy of the content of
 * pMem->z into the new allocation.  pMem must be either a string or
 * blob if bPreserve is true.  If bPreserve is false, any prior content
 * in pMem->z is discarded.
 */
SQLITE_NOINLINE int
sqlite3VdbeMemGrow(Mem * pMem, int n, int bPreserve)
{
	assert(sqlite3VdbeCheckMemInvariants(pMem));
	testcase(pMem->db == 0);

	/* If the bPreserve flag is set to true, then the memory cell must already
	 * contain a valid string or blob value.
	 */
	assert(bPreserve == 0 || pMem->flags & (MEM_Blob | MEM_Str));
	testcase(bPreserve && pMem->z == 0);

	assert(pMem->szMalloc == 0
	       || pMem->szMalloc == sqlite3DbMallocSize(pMem->db,
							pMem->zMalloc));
	if (pMem->szMalloc < n) {
		if (n < 32)
			n = 32;
		if (bPreserve && pMem->szMalloc > 0 && pMem->z == pMem->zMalloc) {
			pMem->z = pMem->zMalloc =
			    sqlite3DbReallocOrFree(pMem->db, pMem->z, n);
			bPreserve = 0;
		} else {
			if (pMem->szMalloc > 0)
				sqlite3DbFree(pMem->db, pMem->zMalloc);
			pMem->zMalloc = sqlite3DbMallocRaw(pMem->db, n);
		}
		if (pMem->zMalloc == 0) {
			sqlite3VdbeMemSetNull(pMem);
			pMem->z = 0;
			pMem->szMalloc = 0;
			return SQLITE_NOMEM_BKPT;
		} else {
			pMem->szMalloc =
			    sqlite3DbMallocSize(pMem->db, pMem->zMalloc);
		}
	}

	if (bPreserve && pMem->z && pMem->z != pMem->zMalloc) {
		memcpy(pMem->zMalloc, pMem->z, pMem->n);
	}
	if ((pMem->flags & MEM_Dyn) != 0) {
		assert(pMem->xDel != 0 && pMem->xDel != SQLITE_DYNAMIC);
		pMem->xDel((void *)(pMem->z));
	}

	pMem->z = pMem->zMalloc;
	pMem->flags &= ~(MEM_Dyn | MEM_Ephem | MEM_Static);
	return SQLITE_OK;
}

/*
 * Change the pMem->zMalloc allocation to be at least szNew bytes.
 * If pMem->zMalloc already meets or exceeds the requested size, this
 * routine is a no-op.
 *
 * Any prior string or blob content in the pMem object may be discarded.
 * The pMem->xDel destructor is called, if it exists.  Though MEM_Str
 * and MEM_Blob values may be discarded, MEM_Int, MEM_Real, and MEM_Null
 * values are preserved.
 *
 * Return SQLITE_OK on success or an error code (probably SQLITE_NOMEM)
 * if unable to complete the resizing.
 */
int
sqlite3VdbeMemClearAndResize(Mem * pMem, int szNew)
{
	assert(szNew > 0);
	assert((pMem->flags & MEM_Dyn) == 0 || pMem->szMalloc == 0);
	if (pMem->szMalloc < szNew) {
		return sqlite3VdbeMemGrow(pMem, szNew, 0);
	}
	assert((pMem->flags & MEM_Dyn) == 0);
	pMem->z = pMem->zMalloc;
	pMem->flags &= (MEM_Null | MEM_Int | MEM_Real);
	return SQLITE_OK;
}

/*
 * Change pMem so that its MEM_Str or MEM_Blob value is stored in
 * MEM.zMalloc, where it can be safely written.
 *
 * Return SQLITE_OK on success or SQLITE_NOMEM if malloc fails.
 */
int
sqlite3VdbeMemMakeWriteable(Mem * pMem)
{
	if ((pMem->flags & (MEM_Str | MEM_Blob)) != 0) {
		if (ExpandBlob(pMem))
			return SQLITE_NOMEM;
		if (pMem->szMalloc == 0 || pMem->z != pMem->zMalloc) {
			if (sqlite3VdbeMemGrow(pMem, pMem->n + 2, 1)) {
				return SQLITE_NOMEM_BKPT;
			}
			pMem->z[pMem->n] = 0;
			pMem->z[pMem->n + 1] = 0;
			pMem->flags |= MEM_Term;
		}
	}
	pMem->flags &= ~MEM_Ephem;
#ifdef SQLITE_DEBUG
	pMem->pScopyFrom = 0;
#endif

	return SQLITE_OK;
}

/*
 * If the given Mem* has a zero-filled tail, turn it into an ordinary
 * blob stored in dynamically allocated space.
 */
#ifndef SQLITE_OMIT_INCRBLOB
int
sqlite3VdbeMemExpandBlob(Mem * pMem)
{
	int nByte;
	assert(pMem->flags & MEM_Zero);
	assert(pMem->flags & MEM_Blob);

	/* Set nByte to the number of bytes required to store the expanded blob. */
	nByte = pMem->n + pMem->u.nZero;
	if (nByte <= 0) {
		nByte = 1;
	}
	if (sqlite3VdbeMemGrow(pMem, nByte, 1)) {
		return SQLITE_NOMEM_BKPT;
	}

	memset(&pMem->z[pMem->n], 0, pMem->u.nZero);
	pMem->n += pMem->u.nZero;
	pMem->flags &= ~(MEM_Zero | MEM_Term);
	return SQLITE_OK;
}
#endif

/*
 * It is already known that pMem contains an unterminated string.
 * Add the zero terminator.
 */
static SQLITE_NOINLINE int
vdbeMemAddTerminator(Mem * pMem)
{
	if (sqlite3VdbeMemGrow(pMem, pMem->n + 2, 1)) {
		return SQLITE_NOMEM_BKPT;
	}
	pMem->z[pMem->n] = 0;
	pMem->z[pMem->n + 1] = 0;
	pMem->flags |= MEM_Term;
	return SQLITE_OK;
}

/*
 * Make sure the given Mem is \u0000 terminated.
 */
int
sqlite3VdbeMemNulTerminate(Mem * pMem)
{
	testcase((pMem->flags & (MEM_Term | MEM_Str)) == (MEM_Term | MEM_Str));
	testcase((pMem->flags & (MEM_Term | MEM_Str)) == 0);
	if ((pMem->flags & (MEM_Term | MEM_Str)) != MEM_Str) {
		return SQLITE_OK;	/* Nothing to do */
	} else {
		return vdbeMemAddTerminator(pMem);
	}
}

/*
 * Add MEM_Str to the set of representations for the given Mem.  Numbers
 * are converted using sqlite3_snprintf().  Converting a BLOB to a string
 * is a no-op.
 *
 * Existing representations MEM_Int and MEM_Real are invalidated if
 * bForce is true but are retained if bForce is false.
 *
 * A MEM_Null value will never be passed to this function. This function is
 * used for converting values to text for returning to the user (i.e. via
 * sqlite3_value_text()), or for ensuring that values to be used as btree
 * keys are strings. In the former case a NULL pointer is returned the
 * user and the latter is an internal programming error.
 */
int
sqlite3VdbeMemStringify(Mem * pMem, u8 bForce)
{
	int fg = pMem->flags;
	const int nByte = 32;

	assert(!(fg & MEM_Zero));
	assert(!(fg & (MEM_Str | MEM_Blob)));
	assert(fg & (MEM_Int | MEM_Real));
	assert(EIGHT_BYTE_ALIGNMENT(pMem));

	if (sqlite3VdbeMemClearAndResize(pMem, nByte)) {
		return SQLITE_NOMEM_BKPT;
	}
	if (fg & MEM_Int) {
		sqlite3_snprintf(nByte, pMem->z, "%lld", pMem->u.i);
	} else {
		assert(fg & MEM_Real);
		sqlite3_snprintf(nByte, pMem->z, "%!.15g", pMem->u.r);
	}
	pMem->n = sqlite3Strlen30(pMem->z);
	pMem->flags |= MEM_Str | MEM_Term;
	if (bForce)
		pMem->flags &= ~(MEM_Int | MEM_Real);
	return SQLITE_OK;
}

/*
 * Memory cell pMem contains the context of an aggregate function.
 * This routine calls the finalize method for that function.  The
 * result of the aggregate is stored back into pMem.
 *
 * Return SQLITE_ERROR if the finalizer reports an error.  SQLITE_OK
 * otherwise.
 */
int
sqlite3VdbeMemFinalize(Mem * pMem, FuncDef * pFunc)
{
	int rc = SQLITE_OK;
	if (ALWAYS(pFunc && pFunc->xFinalize)) {
		sqlite3_context ctx;
		Mem t;
		assert((pMem->flags & MEM_Null) != 0 || pFunc == pMem->u.pDef);
		memset(&ctx, 0, sizeof(ctx));
		memset(&t, 0, sizeof(t));
		t.flags = MEM_Null;
		t.db = pMem->db;
		ctx.pOut = &t;
		ctx.pMem = pMem;
		ctx.pFunc = pFunc;
		pFunc->xFinalize(&ctx);	/* IMP: R-24505-23230 */
		assert((pMem->flags & MEM_Dyn) == 0);
		if (pMem->szMalloc > 0)
			sqlite3DbFree(pMem->db, pMem->zMalloc);
		memcpy(pMem, &t, sizeof(t));
		rc = ctx.isError;
	}
	return rc;
}

/*
 * If the memory cell contains a value that must be freed by
 * invoking the external callback in Mem.xDel, then this routine
 * will free that value.  It also sets Mem.flags to MEM_Null.
 *
 * This is a helper routine for sqlite3VdbeMemSetNull() and
 * for sqlite3VdbeMemRelease().  Use those other routines as the
 * entry point for releasing Mem resources.
 */
static SQLITE_NOINLINE void
vdbeMemClearExternAndSetNull(Mem * p)
{
	assert(VdbeMemDynamic(p));
	if (p->flags & MEM_Agg) {
		sqlite3VdbeMemFinalize(p, p->u.pDef);
		assert((p->flags & MEM_Agg) == 0);
		testcase(p->flags & MEM_Dyn);
	}
	if (p->flags & MEM_Dyn) {
		assert(p->xDel != SQLITE_DYNAMIC && p->xDel != 0);
		p->xDel((void *)p->z);
	} else if (p->flags & MEM_Frame) {
		VdbeFrame *pFrame = p->u.pFrame;
		pFrame->pParent = pFrame->v->pDelFrame;
		pFrame->v->pDelFrame = pFrame;
	}
	p->flags = MEM_Null;
}

/*
 * Release memory held by the Mem p, both external memory cleared
 * by p->xDel and memory in p->zMalloc.
 *
 * This is a helper routine invoked by sqlite3VdbeMemRelease() in
 * the unusual case where there really is memory in p that needs
 * to be freed.
 */
static SQLITE_NOINLINE void
vdbeMemClear(Mem * p)
{
	if (VdbeMemDynamic(p)) {
		vdbeMemClearExternAndSetNull(p);
	}
	if (p->szMalloc) {
		sqlite3DbFree(p->db, p->zMalloc);
		p->szMalloc = 0;
	}
	p->z = 0;
}

/*
 * Release any memory resources held by the Mem.  Both the memory that is
 * free by Mem.xDel and the Mem.zMalloc allocation are freed.
 *
 * Use this routine prior to clean up prior to abandoning a Mem, or to
 * reset a Mem back to its minimum memory utilization.
 *
 * Use sqlite3VdbeMemSetNull() to release just the Mem.xDel space
 * prior to inserting new content into the Mem.
 */
void
sqlite3VdbeMemRelease(Mem * p)
{
	assert(sqlite3VdbeCheckMemInvariants(p));
	if (VdbeMemDynamic(p) || p->szMalloc) {
		vdbeMemClear(p);
	}
}

/*
 * Convert a 64-bit IEEE double into a 64-bit signed integer.
 * If the double is out of range of a 64-bit signed integer then
 * return the closest available 64-bit signed integer.
 */
static i64
doubleToInt64(double r)
{
#ifdef SQLITE_OMIT_FLOATING_POINT
	/* When floating-point is omitted, double and int64 are the same thing */
	return r;
#else
	/*
	 * Many compilers we encounter do not define constants for the
	 * minimum and maximum 64-bit integers, or they define them
	 * inconsistently.  And many do not understand the "LL" notation.
	 * So we define our own static constants here using nothing
	 * larger than a 32-bit integer constant.
	 */
	static const i64 maxInt = LARGEST_INT64;
	static const i64 minInt = SMALLEST_INT64;

	if (r <= (double)minInt) {
		return minInt;
	} else if (r >= (double)maxInt) {
		return maxInt;
	} else {
		return (i64) r;
	}
#endif
}

/*
 * Return some kind of integer value which is the best we can do
 * at representing the value that *pMem describes as an integer.
 * If pMem is an integer, then the value is exact.  If pMem is
 * a floating-point then the value returned is the integer part.
 * If pMem is a string or blob, then we make an attempt to convert
 * it into an integer and return that.  If pMem represents an
 * an SQL-NULL value, return 0.
 *
 * If pMem represents a string value, its encoding might be changed.
 */
i64
sqlite3VdbeIntValue(Mem * pMem)
{
	int flags;
	assert(EIGHT_BYTE_ALIGNMENT(pMem));
	flags = pMem->flags;
	if (flags & MEM_Int) {
		return pMem->u.i;
	} else if (flags & MEM_Real) {
		return doubleToInt64(pMem->u.r);
	} else if (flags & (MEM_Str | MEM_Blob)) {
		int64_t value = 0;
		assert(pMem->z || pMem->n == 0);
		sql_atoi64(pMem->z, &value, pMem->n);
		return value;
	} else {
		return 0;
	}
}

/*
 * Return the best representation of pMem that we can get into a
 * double.  If pMem is already a double or an integer, return its
 * value.  If it is a string or blob, try to convert it to a double.
 * If it is a NULL, return 0.0.
 */
double
sqlite3VdbeRealValue(Mem * pMem)
{
	assert(EIGHT_BYTE_ALIGNMENT(pMem));
	if (pMem->flags & MEM_Real) {
		return pMem->u.r;
	} else if (pMem->flags & MEM_Int) {
		return (double)pMem->u.i;
	} else if (pMem->flags & (MEM_Str | MEM_Blob)) {
		/* (double)0 In case of SQLITE_OMIT_FLOATING_POINT... */
		double val = (double)0;
		sqlite3AtoF(pMem->z, &val, pMem->n);
		return val;
	} else {
		/* (double)0 In case of SQLITE_OMIT_FLOATING_POINT... */
		return (double)0;
	}
}

/*
 * The MEM structure is already a MEM_Real.  Try to also make it a
 * MEM_Int if we can.
 */
void
sqlite3VdbeIntegerAffinity(Mem * pMem)
{
	i64 ix;
	assert(pMem->flags & MEM_Real);
	assert(EIGHT_BYTE_ALIGNMENT(pMem));

	ix = doubleToInt64(pMem->u.r);

	/* Only mark the value as an integer if
	 *
	 *    (1) the round-trip conversion real->int->real is a no-op, and
	 *    (2) The integer is neither the largest nor the smallest
	 *        possible integer (ticket #3922)
	 *
	 * The second and third terms in the following conditional enforces
	 * the second condition under the assumption that addition overflow causes
	 * values to wrap around.
	 */
	if (pMem->u.r == ix && ix > SMALLEST_INT64 && ix < LARGEST_INT64) {
		pMem->u.i = ix;
		MemSetTypeFlag(pMem, MEM_Int);
	}
}

/*
 * Convert pMem to type integer.  Invalidate any prior representations.
 */
int
sqlite3VdbeMemIntegerify(Mem * pMem)
{
	assert(EIGHT_BYTE_ALIGNMENT(pMem));

	pMem->u.i = sqlite3VdbeIntValue(pMem);
	MemSetTypeFlag(pMem, MEM_Int);
	return SQLITE_OK;
}

/*
 * Convert pMem so that it is of type MEM_Real.
 * Invalidate any prior representations.
 */
int
sqlite3VdbeMemRealify(Mem * pMem)
{
	assert(EIGHT_BYTE_ALIGNMENT(pMem));

	pMem->u.r = sqlite3VdbeRealValue(pMem);
	MemSetTypeFlag(pMem, MEM_Real);
	return SQLITE_OK;
}

/*
 * Convert pMem so that it has types MEM_Real or MEM_Int or both.
 * Invalidate any prior representations.
 *
 * Every effort is made to force the conversion, even if the input
 * is a string that does not look completely like a number.  Convert
 * as much of the string as we can and ignore the rest.
 */
int
sqlite3VdbeMemNumerify(Mem * pMem)
{
	if ((pMem->flags & (MEM_Int | MEM_Real | MEM_Null)) == 0) {
		assert((pMem->flags & (MEM_Blob | MEM_Str)) != 0);
		if (0 == sql_atoi64(pMem->z, (int64_t *)&pMem->u.i, pMem->n)) {
			MemSetTypeFlag(pMem, MEM_Int);
		} else {
			pMem->u.r = sqlite3VdbeRealValue(pMem);
			MemSetTypeFlag(pMem, MEM_Real);
			sqlite3VdbeIntegerAffinity(pMem);
		}
	}
	assert((pMem->flags & (MEM_Int | MEM_Real | MEM_Null)) != 0);
	pMem->flags &= ~(MEM_Str | MEM_Blob | MEM_Zero);
	return SQLITE_OK;
}

/*
 * Cast the datatype of the value in pMem according to the affinity
 * "aff".  Casting is different from applying affinity in that a cast
 * is forced.  In other words, the value is converted into the desired
 * affinity even if that results in loss of data.  This routine is
 * used (for example) to implement the SQL "cast()" operator.
 */
void
sqlite3VdbeMemCast(Mem * pMem, u8 aff)
{
	if (pMem->flags & MEM_Null)
		return;
	switch (aff) {
	case AFFINITY_BLOB:{	/* Really a cast to BLOB */
			if ((pMem->flags & MEM_Blob) == 0) {
				sqlite3ValueApplyAffinity(pMem, AFFINITY_TEXT);
				assert(pMem->flags & MEM_Str
				       || pMem->db->mallocFailed);
				if (pMem->flags & MEM_Str)
					MemSetTypeFlag(pMem, MEM_Blob);
			} else {
				pMem->flags &= ~(MEM_TypeMask & ~MEM_Blob);
			}
			break;
		}
	case AFFINITY_NUMERIC:{
			sqlite3VdbeMemNumerify(pMem);
			break;
		}
	case AFFINITY_INTEGER:{
			sqlite3VdbeMemIntegerify(pMem);
			break;
		}
	case AFFINITY_REAL:{
			sqlite3VdbeMemRealify(pMem);
			break;
		}
	default:{
			assert(aff == AFFINITY_TEXT);
			assert(MEM_Str == (MEM_Blob >> 3));
			pMem->flags |= (pMem->flags & MEM_Blob) >> 3;
			sqlite3ValueApplyAffinity(pMem, AFFINITY_TEXT);
			assert(pMem->flags & MEM_Str || pMem->db->mallocFailed);
			pMem->flags &=
			    ~(MEM_Int | MEM_Real | MEM_Blob | MEM_Zero);
			break;
		}
	}
}

/*
 * Initialize bulk memory to be a consistent Mem object.
 *
 * The minimum amount of initialization feasible is performed.
 */
void
sqlite3VdbeMemInit(Mem * pMem, sqlite3 * db, u32 flags)
{
	assert((flags & ~MEM_TypeMask) == 0);
	pMem->flags = flags;
	pMem->db = db;
	pMem->szMalloc = 0;
}

/*
 * Delete any previous value and set the value stored in *pMem to NULL.
 *
 * This routine calls the Mem.xDel destructor to dispose of values that
 * require the destructor.  But it preserves the Mem.zMalloc memory allocation.
 * To free all resources, use sqlite3VdbeMemRelease(), which both calls this
 * routine to invoke the destructor and deallocates Mem.zMalloc.
 *
 * Use this routine to reset the Mem prior to insert a new value.
 *
 * Use sqlite3VdbeMemRelease() to complete erase the Mem prior to abandoning it.
 */
void
sqlite3VdbeMemSetNull(Mem * pMem)
{
	if (VdbeMemDynamic(pMem)) {
		vdbeMemClearExternAndSetNull(pMem);
	} else {
		pMem->flags = MEM_Null;
	}
}

void
sqlite3ValueSetNull(sqlite3_value * p)
{
	sqlite3VdbeMemSetNull((Mem *) p);
}

/*
 * Delete any previous value and set the value to be a BLOB of length
 * n containing all zeros.
 */
void
sqlite3VdbeMemSetZeroBlob(Mem * pMem, int n)
{
	sqlite3VdbeMemRelease(pMem);
	pMem->flags = MEM_Blob | MEM_Zero;
	pMem->n = 0;
	if (n < 0)
		n = 0;
	pMem->u.nZero = n;
	pMem->z = 0;
}

/*
 * The pMem is known to contain content that needs to be destroyed prior
 * to a value change.  So invoke the destructor, then set the value to
 * a 64-bit integer.
 */
static SQLITE_NOINLINE void
vdbeReleaseAndSetInt64(Mem * pMem, i64 val)
{
	sqlite3VdbeMemSetNull(pMem);
	pMem->u.i = val;
	pMem->flags = MEM_Int;
}

/*
 * Delete any previous value and set the value stored in *pMem to val,
 * manifest type INTEGER.
 */
void
sqlite3VdbeMemSetInt64(Mem * pMem, i64 val)
{
	if (VdbeMemDynamic(pMem)) {
		vdbeReleaseAndSetInt64(pMem, val);
	} else {
		pMem->u.i = val;
		pMem->flags = MEM_Int;
	}
}

#ifndef SQLITE_OMIT_FLOATING_POINT
/*
 * Delete any previous value and set the value stored in *pMem to val,
 * manifest type REAL.
 */
void
sqlite3VdbeMemSetDouble(Mem * pMem, double val)
{
	sqlite3VdbeMemSetNull(pMem);
	if (!sqlite3IsNaN(val)) {
		pMem->u.r = val;
		pMem->flags = MEM_Real;
	}
}
#endif

/*
 * Return true if the Mem object contains a TEXT or BLOB that is
 * too large - whose size exceeds SQLITE_MAX_LENGTH.
 */
int
sqlite3VdbeMemTooBig(Mem * p)
{
	assert(p->db != 0);
	if (p->flags & (MEM_Str | MEM_Blob)) {
		int n = p->n;
		if (p->flags & MEM_Zero) {
			n += p->u.nZero;
		}
		return n > p->db->aLimit[SQLITE_LIMIT_LENGTH];
	}
	return 0;
}

#ifdef SQLITE_DEBUG
/*
 * This routine prepares a memory cell for modification by breaking
 * its link to a shallow copy and by marking any current shallow
 * copies of this cell as invalid.
 *
 * This is used for testing and debugging only - to make sure shallow
 * copies are not misused.
 */
void
sqlite3VdbeMemAboutToChange(Vdbe * pVdbe, Mem * pMem)
{
	int i;
	Mem *pX;
	for (i = 0, pX = pVdbe->aMem; i < pVdbe->nMem; i++, pX++) {
		if (pX->pScopyFrom == pMem) {
			pX->flags |= MEM_Undefined;
			pX->pScopyFrom = 0;
		}
	}
	pMem->pScopyFrom = 0;
}
#endif				/* SQLITE_DEBUG */

/*
 * Make an shallow copy of pFrom into pTo.  Prior contents of
 * pTo are freed.  The pFrom->z field is not duplicated.  If
 * pFrom->z is used, then pTo->z points to the same thing as pFrom->z
 * and flags gets srcType (either MEM_Ephem or MEM_Static).
 */
static SQLITE_NOINLINE void
vdbeClrCopy(Mem * pTo, const Mem * pFrom, int eType)
{
	vdbeMemClearExternAndSetNull(pTo);
	assert(!VdbeMemDynamic(pTo));
	sqlite3VdbeMemShallowCopy(pTo, pFrom, eType);
}

void
sqlite3VdbeMemShallowCopy(Mem * pTo, const Mem * pFrom, int srcType)
{
	assert(pTo->db == pFrom->db);
	if (VdbeMemDynamic(pTo)) {
		vdbeClrCopy(pTo, pFrom, srcType);
		return;
	}
	memcpy(pTo, pFrom, MEMCELLSIZE);
	if ((pFrom->flags & MEM_Static) == 0) {
		pTo->flags &= ~(MEM_Dyn | MEM_Static | MEM_Ephem);
		assert(srcType == MEM_Ephem || srcType == MEM_Static);
		pTo->flags |= srcType;
	}
}

/*
 * Make a full copy of pFrom into pTo.  Prior contents of pTo are
 * freed before the copy is made.
 */
int
sqlite3VdbeMemCopy(Mem * pTo, const Mem * pFrom)
{
	int rc = SQLITE_OK;

	if (VdbeMemDynamic(pTo))
		vdbeMemClearExternAndSetNull(pTo);
	memcpy(pTo, pFrom, MEMCELLSIZE);
	pTo->flags &= ~MEM_Dyn;
	if (pTo->flags & (MEM_Str | MEM_Blob)) {
		if (0 == (pFrom->flags & MEM_Static)) {
			pTo->flags |= MEM_Ephem;
			rc = sqlite3VdbeMemMakeWriteable(pTo);
		}
	}

	return rc;
}

/*
 * Transfer the contents of pFrom to pTo. Any existing value in pTo is
 * freed. If pFrom contains ephemeral data, a copy is made.
 *
 * pFrom contains an SQL NULL when this routine returns.
 */
void
sqlite3VdbeMemMove(Mem * pTo, Mem * pFrom)
{
	assert(pFrom->db == 0 || pTo->db == 0 || pFrom->db == pTo->db);

	sqlite3VdbeMemRelease(pTo);
	memcpy(pTo, pFrom, sizeof(Mem));
	pFrom->flags = MEM_Null;
	pFrom->szMalloc = 0;
}

/*
 * Change the value of a Mem to be a string or a BLOB.
 *
 * The memory management strategy depends on the value of the xDel
 * parameter. If the value passed is SQLITE_TRANSIENT, then the
 * string is copied into a (possibly existing) buffer managed by the
 * Mem structure. Otherwise, any existing buffer is freed and the
 * pointer copied.
 *
 * If the string is too large (if it exceeds the SQLITE_LIMIT_LENGTH
 * size limit) then no memory allocation occurs.  If the string can be
 * stored without allocating memory, then it is.  If a memory allocation
 * is required to store the string, then value of pMem is unchanged.  In
 * either case, SQLITE_TOOBIG is returned.
 */
int
sqlite3VdbeMemSetStr(Mem * pMem,	/* Memory cell to set to string value */
		     const char *z,	/* String pointer */
		     int n,	/* Bytes in string, or negative */
		     u8 not_blob,	/* Encoding of z.  0 for BLOBs */
		     void (*xDel) (void *)	/* Destructor function */
    )
{
	int nByte = n;		/* New value for pMem->n */
	int iLimit;		/* Maximum allowed string or blob size */
	u16 flags = 0;		/* New value for pMem->flags */

	/* If z is a NULL pointer, set pMem to contain an SQL NULL. */
	if (!z) {
		sqlite3VdbeMemSetNull(pMem);
		return SQLITE_OK;
	}

	if (pMem->db) {
		iLimit = pMem->db->aLimit[SQLITE_LIMIT_LENGTH];
	} else {
		iLimit = SQLITE_MAX_LENGTH;
	}
	flags = (not_blob == 0 ? MEM_Blob : MEM_Str);
	if (nByte < 0) {
		assert(not_blob != 0);
		nByte = sqlite3Strlen30(z);
		if (nByte > iLimit)
			nByte = iLimit + 1;
		flags |= MEM_Term;
	}

	/* The following block sets the new values of Mem.z and Mem.xDel. It
	 * also sets a flag in local variable "flags" to indicate the memory
	 * management (one of MEM_Dyn or MEM_Static).
	 */
	if (xDel == SQLITE_TRANSIENT) {
		int nAlloc = nByte;
		if (flags & MEM_Term) {
			nAlloc += 1; //SQLITE_UTF8
		}
		if (nByte > iLimit) {
			return SQLITE_TOOBIG;
		}
		testcase(nAlloc == 0);
		testcase(nAlloc == 31);
		testcase(nAlloc == 32);
		if (sqlite3VdbeMemClearAndResize(pMem, MAX(nAlloc, 32))) {
			return SQLITE_NOMEM_BKPT;
		}
		memcpy(pMem->z, z, nAlloc);
	} else if (xDel == SQLITE_DYNAMIC) {
		sqlite3VdbeMemRelease(pMem);
		pMem->zMalloc = pMem->z = (char *)z;
		pMem->szMalloc = sqlite3DbMallocSize(pMem->db, pMem->zMalloc);
	} else {
		sqlite3VdbeMemRelease(pMem);
		pMem->z = (char *)z;
		pMem->xDel = xDel;
		flags |= ((xDel == SQLITE_STATIC) ? MEM_Static : MEM_Dyn);
	}

	pMem->n = nByte;
	pMem->flags = flags;

	if (nByte > iLimit) {
		return SQLITE_TOOBIG;
	}

	return SQLITE_OK;
}

/*
 * Move data out of a btree key or data field and into a Mem structure.
 * The data is payload from the entry that pCur is currently pointing
 * to.  offset and amt determine what portion of the data or key to retrieve.
 * The result is written into the pMem element.
 *
 * The pMem object must have been initialized.  This routine will use
 * pMem->zMalloc to hold the content from the btree, if possible.  New
 * pMem->zMalloc space will be allocated if necessary.  The calling routine
 * is responsible for making sure that the pMem object is eventually
 * destroyed.
 *
 * If this routine fails for any reason (malloc returns NULL or unable
 * to read from the disk) then the pMem is left in an inconsistent state.
 */
static SQLITE_NOINLINE int
vdbeMemFromBtreeResize(BtCursor * pCur,	/* Cursor pointing at record to retrieve. */
		       u32 offset,	/* Offset from the start of data to return bytes from. */
		       u32 amt,	/* Number of bytes to return. */
		       Mem * pMem	/* OUT: Return data in this Mem structure. */
    )
{
	int rc;
	pMem->flags = MEM_Null;
	if (SQLITE_OK == (rc = sqlite3VdbeMemClearAndResize(pMem, amt + 2))) {
		rc = sqlite3CursorPayload(pCur, offset, amt, pMem->z);
		if (rc == SQLITE_OK) {
			pMem->z[amt] = 0;
			pMem->z[amt + 1] = 0;
			pMem->flags = MEM_Blob | MEM_Term;
			pMem->n = (int)amt;
		} else {
			sqlite3VdbeMemRelease(pMem);
		}
	}
	return rc;
}

int
sqlite3VdbeMemFromBtree(BtCursor * pCur,	/* Cursor pointing at record to retrieve. */
			u32 offset,	/* Offset from the start of data to return bytes from. */
			u32 amt,	/* Number of bytes to return. */
			Mem * pMem	/* OUT: Return data in this Mem structure. */
    )
{
	char *zData;		/* Data from the btree layer */
	u32 available = 0;	/* Number of bytes available on the local btree page */
	int rc = SQLITE_OK;	/* Return code */

	assert(sqlite3CursorIsValid(pCur));
	assert(!VdbeMemDynamic(pMem));
	assert(pCur->curFlags & BTCF_TaCursor ||
	       pCur->curFlags & BTCF_TEphemCursor);


	zData = (char *)tarantoolSqlite3PayloadFetch(pCur, &available);
	assert(zData != 0);

	if (offset + amt <= available) {
		pMem->z = &zData[offset];
		pMem->flags = MEM_Blob | MEM_Ephem;
		pMem->n = (int)amt;
	} else {
		rc = vdbeMemFromBtreeResize(pCur, offset, amt, pMem);
	}

	return rc;
}

/*
 * The pVal argument is known to be a value other than NULL.
 * Convert it into a string with encoding enc and return a pointer
 * to a zero-terminated version of that string.
 */
static SQLITE_NOINLINE const void *
valueToText(sqlite3_value * pVal)
{
	assert(pVal != 0);
	assert((pVal->flags & (MEM_Null)) == 0);
	if (pVal->flags & (MEM_Blob | MEM_Str)) {
		if (ExpandBlob(pVal))
			return 0;
		pVal->flags |= MEM_Str;
		sqlite3VdbeMemNulTerminate(pVal);	/* IMP: R-31275-44060 */
	} else {
		sqlite3VdbeMemStringify(pVal, 0);
		assert(0 == (1 & SQLITE_PTR_TO_INT(pVal->z)));
	}
	return pVal->z;
}

/* This function is only available internally, it is not part of the
 * external API. It works in a similar way to sqlite3_value_text(),
 * except the data returned is in the encoding specified by the second
 * parameter, which must be one of SQLITE_UTF16BE, SQLITE_UTF16LE or
 * SQLITE_UTF8.
 *
 * (2006-02-16:)  The enc value can be or-ed with SQLITE_UTF16_ALIGNED.
 * If that is the case, then the result must be aligned on an even byte
 * boundary.
 */
const void *
sqlite3ValueText(sqlite3_value * pVal)
{
	if (!pVal)
		return 0;
	if ((pVal->flags & (MEM_Str | MEM_Term)) == (MEM_Str | MEM_Term)) {
		return pVal->z;
	}
	if (pVal->flags & MEM_Null) {
		return 0;
	}
	return valueToText(pVal);
}

/*
 * Create a new sqlite3_value object.
 */
sqlite3_value *
sqlite3ValueNew(sqlite3 * db)
{
	Mem *p = sqlite3DbMallocZero(db, sizeof(*p));
	if (p) {
		p->flags = MEM_Null;
		p->db = db;
	}
	return p;
}

/*
 * Context object passed by sqlite3Stat4ProbeSetValue() through to
 * valueNew(). See comments above valueNew() for details.
 */
struct ValueNewStat4Ctx {
	Parse *pParse;
	Index *pIdx;
	UnpackedRecord **ppRec;
	int iVal;
};

/*
 * Allocate and return a pointer to a new sqlite3_value object. If
 * the second argument to this function is NULL, the object is allocated
 * by calling sqlite3ValueNew().
 *
 * Otherwise, if the second argument is non-zero, then this function is
 * being called indirectly by sqlite3Stat4ProbeSetValue(). If it has not
 * already been allocated, allocate the UnpackedRecord structure that
 * that function will return to its caller here. Then return a pointer to
 * an sqlite3_value within the UnpackedRecord.a[] array.
 */
static sqlite3_value *
valueNew(sqlite3 * db, struct ValueNewStat4Ctx *p)
{
	if (p) {
		UnpackedRecord *pRec = p->ppRec[0];

		if (pRec == 0) {
			Index *pIdx = p->pIdx;	/* Index being probed */
			int nByte;	/* Bytes of space to allocate */
			int i;	/* Counter variable */
			int part_count = pIdx->def->key_def->part_count;

			nByte = sizeof(Mem) * part_count +
				ROUND8(sizeof(UnpackedRecord));
			pRec =
			    (UnpackedRecord *) sqlite3DbMallocZero(db, nByte);
			if (pRec == NULL)
				return NULL;
			pRec->key_def = key_def_dup(pIdx->def->key_def);
			if (pRec->key_def == NULL) {
				sqlite3DbFree(db, pRec);
				sqlite3OomFault(db);
				return NULL;
			}
			pRec->aMem = (Mem *)((char *) pRec +
					     ROUND8(sizeof(UnpackedRecord)));
			for (i = 0; i < (int) part_count; i++) {
				pRec->aMem[i].flags = MEM_Null;
				pRec->aMem[i].db = db;
			}
			p->ppRec[0] = pRec;
		}

		pRec->nField = p->iVal + 1;
		return &pRec->aMem[p->iVal];
	}

	return sqlite3ValueNew(db);
}

/*
 * The expression object indicated by the second argument is guaranteed
 * to be a scalar SQL function. If
 *
 *   * all function arguments are SQL literals,
 *   * one of the SQLITE_FUNC_CONSTANT or _SLOCHNG function flags is set, and
 *   * the SQLITE_FUNC_NEEDCOLL function flag is not set,
 *
 * then this routine attempts to invoke the SQL function. Assuming no
 * error occurs, output parameter (*ppVal) is set to point to a value
 * object containing the result before returning SQLITE_OK.
 *
 * Affinity aff is applied to the result of the function before returning.
 * If the result is a text value, the sqlite3_value object uses encoding
 * enc.
 *
 * If the conditions above are not met, this function returns SQLITE_OK
 * and sets (*ppVal) to NULL. Or, if an error occurs, (*ppVal) is set to
 * NULL and an SQLite error code returned.
 */
static int
valueFromFunction(sqlite3 * db,	/* The database connection */
		  Expr * p,	/* The expression to evaluate */
		  u8 aff,	/* Affinity to use */
		  sqlite3_value ** ppVal,	/* Write the new value here */
		  struct ValueNewStat4Ctx *pCtx	/* Second argument for valueNew() */
    )
{
	sqlite3_context ctx;	/* Context object for function invocation */
	sqlite3_value **apVal = 0;	/* Function arguments */
	int nVal = 0;		/* Size of apVal[] array */
	FuncDef *pFunc = 0;	/* Function definition */
	sqlite3_value *pVal = 0;	/* New value */
	int rc = SQLITE_OK;	/* Return code */
	ExprList *pList = 0;	/* Function arguments */
	int i;			/* Iterator variable */

	assert(pCtx != 0);
	assert((p->flags & EP_TokenOnly) == 0);
	pList = p->x.pList;
	if (pList)
		nVal = pList->nExpr;
	pFunc = sqlite3FindFunction(db, p->u.zToken, nVal, 0);
	assert(pFunc);
	if ((pFunc->funcFlags & (SQLITE_FUNC_CONSTANT | SQLITE_FUNC_SLOCHNG)) ==
	    0 || (pFunc->funcFlags & SQLITE_FUNC_NEEDCOLL)
	    ) {
		return SQLITE_OK;
	}

	if (pList) {
		apVal =
		    (sqlite3_value **) sqlite3DbMallocZero(db,
							   sizeof(apVal[0]) *
							   nVal);
		if (apVal == 0) {
			rc = SQLITE_NOMEM_BKPT;
			goto value_from_function_out;
		}
		for (i = 0; i < nVal; i++) {
			rc = sqlite3ValueFromExpr(db, pList->a[i].pExpr,
						  aff, &apVal[i]);
			if (apVal[i] == 0 || rc != SQLITE_OK)
				goto value_from_function_out;
		}
	}

	pVal = valueNew(db, pCtx);
	if (pVal == 0) {
		rc = SQLITE_NOMEM_BKPT;
		goto value_from_function_out;
	}

	assert(pCtx->pParse->rc == SQLITE_OK);
	memset(&ctx, 0, sizeof(ctx));
	ctx.pOut = pVal;
	ctx.pFunc = pFunc;
	pFunc->xSFunc(&ctx, nVal, apVal);
	if (ctx.isError) {
		rc = ctx.isError;
		sqlite3ErrorMsg(pCtx->pParse, "%s", sqlite3_value_text(pVal));
	} else {
		sqlite3ValueApplyAffinity(pVal, aff);
		assert(rc == SQLITE_OK);
	}
	pCtx->pParse->rc = rc;

 value_from_function_out:
	if (rc != SQLITE_OK) {
		pVal = 0;
	}
	if (apVal) {
		for (i = 0; i < nVal; i++) {
			sqlite3ValueFree(apVal[i]);
		}
		sqlite3DbFree(db, apVal);
	}

	*ppVal = pVal;
	return rc;
}

/*
 * Extract a value from the supplied expression in the manner described
 * above sqlite3ValueFromExpr(). Allocate the sqlite3_value object
 * using valueNew().
 *
 * If pCtx is NULL and an error occurs after the sqlite3_value object
 * has been allocated, it is freed before returning. Or, if pCtx is not
 * NULL, it is assumed that the caller will free any allocated object
 * in all cases.
 */
static int
valueFromExpr(sqlite3 * db,	/* The database connection */
	      Expr * pExpr,	/* The expression to evaluate */
	      u8 affinity,	/* Affinity to use */
	      sqlite3_value ** ppVal,	/* Write the new value here */
	      struct ValueNewStat4Ctx *pCtx	/* Second argument for valueNew() */
    )
{
	int op;
	char *zVal = 0;
	sqlite3_value *pVal = 0;
	int negInt = 1;
	const char *zNeg = "";
	int rc = SQLITE_OK;

	assert(pExpr != 0);
	while ((op = pExpr->op) == TK_UPLUS || op == TK_SPAN)
		pExpr = pExpr->pLeft;
	if (NEVER(op == TK_REGISTER))
		op = pExpr->op2;

	/* Compressed expressions only appear when parsing the DEFAULT clause
	 * on a table column definition, and hence only when pCtx==0.  This
	 * check ensures that an EP_TokenOnly expression is never passed down
	 * into valueFromFunction().
	 */
	assert((pExpr->flags & EP_TokenOnly) == 0 || pCtx == 0);

	if (op == TK_CAST) {
		u8 aff = sqlite3AffinityType(pExpr->u.zToken, 0);
		rc = valueFromExpr(db, pExpr->pLeft, aff, ppVal, pCtx);
		testcase(rc != SQLITE_OK);
		if (*ppVal) {
			sqlite3VdbeMemCast(*ppVal, aff);
			sqlite3ValueApplyAffinity(*ppVal, affinity);
		}
		return rc;
	}

	/* Handle negative integers in a single step.  This is needed in the
	 * case when the value is -9223372036854775808.
	 */
	if (op == TK_UMINUS
	    && (pExpr->pLeft->op == TK_INTEGER
		|| pExpr->pLeft->op == TK_FLOAT)) {
		pExpr = pExpr->pLeft;
		op = pExpr->op;
		negInt = -1;
		zNeg = "-";
	}

	if (op == TK_STRING || op == TK_FLOAT || op == TK_INTEGER) {
		pVal = valueNew(db, pCtx);
		if (pVal == 0)
			goto no_mem;
		if (ExprHasProperty(pExpr, EP_IntValue)) {
			sqlite3VdbeMemSetInt64(pVal,
					       (i64) pExpr->u.iValue * negInt);
		} else {
			zVal =
			    sqlite3MPrintf(db, "%s%s", zNeg, pExpr->u.zToken);
			if (zVal == 0)
				goto no_mem;
			sqlite3ValueSetStr(pVal, -1, zVal, SQLITE_DYNAMIC);
		}
		if ((op == TK_INTEGER || op == TK_FLOAT)
		    && affinity == AFFINITY_BLOB) {
			sqlite3ValueApplyAffinity(pVal, AFFINITY_NUMERIC);
		} else {
			sqlite3ValueApplyAffinity(pVal, affinity);
		}
		if (pVal->flags & (MEM_Int | MEM_Real))
			pVal->flags &= ~MEM_Str;
	} else if (op == TK_UMINUS) {
		/* This branch happens for multiple negative signs.  Ex: -(-5) */
		if (SQLITE_OK ==
		    sqlite3ValueFromExpr(db, pExpr->pLeft, affinity, &pVal)
		    && pVal != 0) {
			sqlite3VdbeMemNumerify(pVal);
			if (pVal->flags & MEM_Real) {
				pVal->u.r = -pVal->u.r;
			} else if (pVal->u.i == SMALLEST_INT64) {
				pVal->u.r = -(double)SMALLEST_INT64;
				MemSetTypeFlag(pVal, MEM_Real);
			} else {
				pVal->u.i = -pVal->u.i;
			}
			sqlite3ValueApplyAffinity(pVal, affinity);
		}
	} else if (op == TK_NULL) {
		pVal = valueNew(db, pCtx);
		if (pVal == 0)
			goto no_mem;
		sqlite3VdbeMemNumerify(pVal);
	}
#ifndef SQLITE_OMIT_BLOB_LITERAL
	else if (op == TK_BLOB) {
		int nVal;
		assert(pExpr->u.zToken[0] == 'x' || pExpr->u.zToken[0] == 'X');
		assert(pExpr->u.zToken[1] == '\'');
		pVal = valueNew(db, pCtx);
		if (!pVal)
			goto no_mem;
		zVal = &pExpr->u.zToken[2];
		nVal = sqlite3Strlen30(zVal) - 1;
		assert(zVal[nVal] == '\'');
		sqlite3VdbeMemSetStr(pVal, sqlite3HexToBlob(db, zVal, nVal),
				     nVal / 2, 0, SQLITE_DYNAMIC);
	}
#endif

	else if (op == TK_FUNCTION && pCtx != 0) {
		rc = valueFromFunction(db, pExpr, affinity, &pVal, pCtx);
	}

	*ppVal = pVal;
	return rc;

 no_mem:
	sqlite3OomFault(db);
	sqlite3DbFree(db, zVal);
	assert(*ppVal == 0);
	if (pCtx == 0)
		sqlite3ValueFree(pVal);

	return SQLITE_NOMEM_BKPT;
}

/*
 * Create a new sqlite3_value object, containing the value of pExpr.
 *
 * This only works for very simple expressions that consist of one constant
 * token (i.e. "5", "5.1", "'a string'"). If the expression can
 * be converted directly into a value, then the value is allocated and
 * a pointer written to *ppVal. The caller is responsible for deallocating
 * the value by passing it to sqlite3ValueFree() later on. If the expression
 * cannot be converted to a value, then *ppVal is set to NULL.
 */
int
sqlite3ValueFromExpr(sqlite3 * db,	/* The database connection */
		     Expr * pExpr,	/* The expression to evaluate */
		     u8 affinity,	/* Affinity to use */
		     sqlite3_value ** ppVal	/* Write the new value here */
    )
{
	return pExpr ? valueFromExpr(db, pExpr, affinity, ppVal, 0) : 0;
}

/*
 * The implementation of the sqlite_record() function. This function accepts
 * a single argument of any type. The return value is a formatted database
 * record (a blob) containing the argument value.
 *
 * This is used to convert the value stored in the 'sample' column of the
 * sql_stat4 table to the record format SQLite uses internally.
 */
static void
recordFunc(sqlite3_context * context, int argc, sqlite3_value ** argv)
{
	const int file_format = 1;
	u32 iSerial;		/* Serial type */
	int nSerial;		/* Bytes of space for iSerial as varint */
	u32 nVal;		/* Bytes of space required for argv[0] */
	int nRet;
	sqlite3 *db;
	u8 *aRet;

	UNUSED_PARAMETER(argc);
	iSerial = sqlite3VdbeSerialType(argv[0], file_format, &nVal);
	nSerial = sqlite3VarintLen(iSerial);
	db = sqlite3_context_db_handle(context);

	nRet = 1 + nSerial + nVal;
	aRet = sqlite3DbMallocRawNN(db, nRet);
	if (aRet == 0) {
		sqlite3_result_error_nomem(context);
	} else {
		aRet[0] = nSerial + 1;
		putVarint32(&aRet[1], iSerial);
		sqlite3VdbeSerialPut(&aRet[1 + nSerial], argv[0], iSerial);
		sqlite3_result_blob(context, aRet, nRet, SQLITE_TRANSIENT);
		sqlite3DbFree(db, aRet);
	}
}

/*
 * Register built-in functions used to help read ANALYZE data.
 */
void
sqlite3AnalyzeFunctions(void)
{
	static FuncDef aAnalyzeTableFuncs[] = {
		FUNCTION(sqlite_record, 1, 0, 0, recordFunc, 0),
	};
	sqlite3InsertBuiltinFuncs(aAnalyzeTableFuncs,
				  ArraySize(aAnalyzeTableFuncs));
}

/*
 * Attempt to extract a value from pExpr and use it to construct *ppVal.
 *
 * If pAlloc is not NULL, then an UnpackedRecord object is created for
 * pAlloc if one does not exist and the new value is added to the
 * UnpackedRecord object.
 *
 * A value is extracted in the following cases:
 *
 *  * (pExpr==0). In this case the value is assumed to be an SQL NULL,
 *
 *  * The expression is a bound variable, and this is a reprepare, or
 *
 *  * The expression is a literal value.
 *
 * On success, *ppVal is made to point to the extracted value.  The caller
 * is responsible for ensuring that the value is eventually freed.
 */
static int
stat4ValueFromExpr(Parse * pParse,	/* Parse context */
		   Expr * pExpr,	/* The expression to extract a value from */
		   u8 affinity,	/* Affinity to use */
		   struct ValueNewStat4Ctx *pAlloc,	/* How to allocate space.  Or NULL */
		   sqlite3_value ** ppVal	/* OUT: New value object (or NULL) */
    )
{
	int rc = SQLITE_OK;
	sqlite3_value *pVal = 0;
	sqlite3 *db = pParse->db;

	/* Skip over any TK_COLLATE nodes */
	pExpr = sqlite3ExprSkipCollate(pExpr);

	if (!pExpr) {
		pVal = valueNew(db, pAlloc);
		if (pVal) {
			sqlite3VdbeMemSetNull((Mem *) pVal);
		}
	} else if (pExpr->op == TK_VARIABLE
		   || NEVER(pExpr->op == TK_REGISTER
			    && pExpr->op2 == TK_VARIABLE)
	    ) {
		Vdbe *v;
		int iBindVar = pExpr->iColumn;
		sqlite3VdbeSetVarmask(pParse->pVdbe, iBindVar);
		if ((v = pParse->pReprepare) != 0) {
			pVal = valueNew(db, pAlloc);
			if (pVal) {
				rc = sqlite3VdbeMemCopy((Mem *) pVal,
							&v->aVar[iBindVar - 1]);
				if (rc == SQLITE_OK) {
					sqlite3ValueApplyAffinity(pVal,
								  affinity);
				}
				pVal->db = pParse->db;
			}
		}
	} else {
		rc = valueFromExpr(db, pExpr, affinity, &pVal, pAlloc);
	}

	assert(pVal == 0 || pVal->db == db);
	*ppVal = pVal;
	return rc;
}

/*
 * This function is used to allocate and populate UnpackedRecord
 * structures intended to be compared against sample index keys stored
 * in the sql_stat4 table.
 *
 * A single call to this function populates zero or more fields of the
 * record starting with field iVal (fields are numbered from left to
 * right starting with 0). A single field is populated if:
 *
 *  * (pExpr==0). In this case the value is assumed to be an SQL NULL,
 *
 *  * The expression is a bound variable, and this is a reprepare, or
 *
 *  * The sqlite3ValueFromExpr() function is able to extract a value
 *    from the expression (i.e. the expression is a literal value).
 *
 * Or, if pExpr is a TK_VECTOR, one field is populated for each of the
 * vector components that match either of the two latter criteria listed
 * above.
 *
 * Before any value is appended to the record, the affinity of the
 * corresponding column within index pIdx is applied to it. Before
 * this function returns, output parameter *pnExtract is set to the
 * number of values appended to the record.
 *
 * When this function is called, *ppRec must either point to an object
 * allocated by an earlier call to this function, or must be NULL. If it
 * is NULL and a value can be successfully extracted, a new UnpackedRecord
 * is allocated (and *ppRec set to point to it) before returning.
 *
 * Unless an error is encountered, SQLITE_OK is returned. It is not an
 * error if a value cannot be extracted from pExpr. If an error does
 * occur, an SQLite error code is returned.
 */
int
sqlite3Stat4ProbeSetValue(Parse * pParse,	/* Parse context */
			  Index * pIdx,	/* Index being probed */
			  UnpackedRecord ** ppRec,	/* IN/OUT: Probe record */
			  Expr * pExpr,	/* The expression to extract a value from */
			  int nElem,	/* Maximum number of values to append */
			  int iVal,	/* Array element to populate */
			  int *pnExtract	/* OUT: Values appended to the record */
    )
{
	int rc = SQLITE_OK;
	int nExtract = 0;

	if (pExpr == 0 || pExpr->op != TK_SELECT) {
		int i;
		struct ValueNewStat4Ctx alloc;

		alloc.pParse = pParse;
		alloc.pIdx = pIdx;
		alloc.ppRec = ppRec;

		for (i = 0; i < nElem; i++) {
			sqlite3_value *pVal = 0;
			Expr *pElem =
			    (pExpr ? sqlite3VectorFieldSubexpr(pExpr, i) : 0);
			u8 aff =
			    sqlite3IndexColumnAffinity(pParse->db, pIdx,
						       iVal + i);
			alloc.iVal = iVal + i;
			rc = stat4ValueFromExpr(pParse, pElem, aff, &alloc,
						&pVal);
			if (!pVal)
				break;
			nExtract++;
		}
	}

	*pnExtract = nExtract;
	return rc;
}

/*
 * Attempt to extract a value from expression pExpr using the methods
 * as described for sqlite3Stat4ProbeSetValue() above.
 *
 * If successful, set *ppVal to point to a new value object and return
 * SQLITE_OK. If no value can be extracted, but no other error occurs
 * (e.g. OOM), return SQLITE_OK and set *ppVal to NULL. Or, if an error
 * does occur, return an SQLite error code. The final value of *ppVal
 * is undefined in this case.
 */
int
sqlite3Stat4ValueFromExpr(Parse * pParse,	/* Parse context */
			  Expr * pExpr,	/* The expression to extract a value from */
			  u8 affinity,	/* Affinity to use */
			  sqlite3_value ** ppVal	/* OUT: New value object (or NULL) */
    )
{
	return stat4ValueFromExpr(pParse, pExpr, affinity, 0, ppVal);
}

int
sql_stat4_column(struct sqlite3 *db, const char *record, uint32_t col_num,
		 sqlite3_value **res)
{
	/* Write result into this Mem object. */
	struct Mem *mem = *res;
	const char *a = record;
	assert(mp_typeof(a[0]) == MP_ARRAY);
	uint32_t col_cnt = mp_decode_array(&a);
	assert(col_cnt > col_num);
	for (uint32_t i = 0; i < col_num; i++)
		mp_next(&a);
	if (mem == NULL) {
		mem = sqlite3ValueNew(db);
		*res = mem;
		if (mem == NULL) {
			diag_set(OutOfMemory, sizeof(struct Mem),
				 "sqlite3ValueNew", "mem");
			return -1;
		}
	}
	sqlite3VdbeMsgpackGet((const unsigned char *) a, mem);
	return 0;
}

/*
 * Unless it is NULL, the argument must be an UnpackedRecord object returned
 * by an earlier call to sqlite3Stat4ProbeSetValue(). This call deletes
 * the object.
 */
void
sqlite3Stat4ProbeFree(UnpackedRecord * pRec)
{
	if (pRec != NULL) {
		int part_count = pRec->key_def->part_count;
		struct Mem *aMem = pRec->aMem;
		for (int i = 0; i < part_count; i++)
			sqlite3VdbeMemRelease(&aMem[i]);
		sqlite3DbFree(aMem[0].db, pRec);
	}
}

/*
 * Change the string value of an sqlite3_value object
 */
void
sqlite3ValueSetStr(sqlite3_value * v,	/* Value to be set */
		   int n,	/* Length of string z */
		   const void *z,	/* Text of the new string */
		   void (*xDel) (void *)	/* Destructor for the string */
    )
{
	if (v)
		sqlite3VdbeMemSetStr((Mem *) v, z, n, 1, xDel);
}

/*
 * Free an sqlite3_value object
 */
void
sqlite3ValueFree(sqlite3_value * v)
{
	if (!v)
		return;
	sqlite3VdbeMemRelease((Mem *) v);
	sqlite3DbFree(((Mem *) v)->db, v);
}

/*
 * The sqlite3ValueBytes() routine returns the number of bytes in the
 * sqlite3_value object assuming that it uses the encoding "enc".
 * The valueBytes() routine is a helper function.
 */
static SQLITE_NOINLINE int
valueBytes(sqlite3_value * pVal)
{
	return valueToText(pVal) != 0 ? pVal->n : 0;
}

int
sqlite3ValueBytes(sqlite3_value * pVal)
{
	Mem *p = (Mem *) pVal;
	assert((p->flags & MEM_Null) == 0
	       || (p->flags & (MEM_Str | MEM_Blob)) == 0);
	if ((p->flags & MEM_Str) != 0) {
		return p->n;
	}
	if ((p->flags & MEM_Blob) != 0) {
		if (p->flags & MEM_Zero) {
			return p->n + p->u.nZero;
		} else {
			return p->n;
		}
	}
	if (p->flags & MEM_Null)
		return 0;
	return valueBytes(pVal);
}
