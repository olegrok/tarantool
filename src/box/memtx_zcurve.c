/*
 * Copyright 2010-2016, Tarantool AUTHORS, please see AUTHORS file.
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

#include <small/mempool.h>
#include <small/small.h>
#include <salad/zcurve.h>

#include "memtx_zcurve.h"
#include "memtx_engine.h"
#include "space.h"
#include "schema.h" /* space_cache_find() */
#include "errinj.h"
#include "memory.h"
#include "fiber.h"
#include "tuple.h"

/**
 * Struct that is used as a elem in BPS tree definition.
 */
struct memtx_zcurve_data {
    /** Z-address. Read here: https://en.wikipedia.org/wiki/Z-order_curve */
	z_address *z_address;
	/* Tuple that this node is represents. */
	struct tuple *tuple;
};

/**
 * Test whether BPS tree elements are identical i.e. represent
 * the same tuple at the same position in the tree.
 * @param a - First BPS tree element to compare.
 * @param b - Second BPS tree element to compare.
 * @retval true - When elements a and b are identical.
 * @retval false - Otherwise.
 */
static bool
memtx_zcurve_data_identical(const struct memtx_zcurve_data *a,
			  const struct memtx_zcurve_data *b)
{
	return a->tuple == b->tuple;
}

/**
 * BPS tree element vs key comparator.
 * Defined in header in order to allow compiler to inline it.
 * @param element - tuple with key to compare.
 * @param key_data - key to compare with.
 * @param def - key definition.
 * @retval 0  if tuple == key in terms of def.
 * @retval <0 if tuple < key in terms of def.
 * @retval >0 if tuple > key in terms of def.
 */
static inline int
memtx_zcurve_compare_key(const struct memtx_zcurve_data *element,
						 z_address* key_data)
{
    assert(element->tuple != NULL && element->z_address != NULL);
    return z_value_cmp(element->z_address, key_data);
}

static inline int
memtx_zcurve_elem_compare(const struct memtx_zcurve_data *elem_a,
                          const struct memtx_zcurve_data *elem_b)
{
    return z_value_cmp(elem_a->z_address, elem_b->z_address);
}

#define BPS_TREE_NAME memtx_zcurve
#define BPS_TREE_BLOCK_SIZE (512)
#define BPS_TREE_EXTENT_SIZE MEMTX_EXTENT_SIZE
#define BPS_TREE_COMPARE(a, b, arg) memtx_zcurve_elem_compare(&a, &b)
#define BPS_TREE_COMPARE_KEY(a, b, arg) memtx_zcurve_compare_key(&a, b)
#define BPS_TREE_IDENTICAL(a, b) memtx_zcurve_data_identical(&a, &b)
#define bps_tree_elem_t struct memtx_zcurve_data
#define bps_tree_key_t z_address *
#define bps_tree_arg_t struct key_def *

#include "salad/bps_tree.h"

#undef BPS_TREE_NAME
#undef BPS_TREE_BLOCK_SIZE
#undef BPS_TREE_EXTENT_SIZE
#undef BPS_TREE_COMPARE
#undef BPS_TREE_COMPARE_KEY
#undef BPS_TREE_IDENTICAL
#undef bps_tree_elem_t
#undef bps_tree_key_t
#undef bps_tree_arg_t

struct memtx_zcurve_index {
	struct index base;
	struct memtx_zcurve tree;
	struct memtx_zcurve_data *build_array;
	size_t build_array_size, build_array_alloc_size;
	struct memtx_gc_task gc_task;
	struct memtx_zcurve_iterator gc_iterator;
};

/* {{{ Utilities. *************************************************/

static int
memtx_zcurve_qcompare(const void* a, const void *b)
{
	const struct memtx_zcurve_data *data_a = a;
	const struct memtx_zcurve_data *data_b = b;
	assert(data_a != NULL && data_a->z_address != NULL);
	assert(data_b != NULL && data_b->z_address != NULL);
	return z_value_cmp(data_a->z_address, data_b->z_address);
}

static uint64_t
str_to_key_part(const char *src, size_t len)
{
	uint64_t result = 0;
	const size_t n = sizeof(result);
	char *dest = (void*)&result;

	for (size_t i = 0; i < n && i < len; i++)
		dest[n - i - 1] = src[i];

	return result;
}

static uint64_t
toggle_high_bit(uint64_t key_part)
{
    key_part ^= (1ULL << 63ULL);
    return key_part;
}

static uint64_t
decode_uint(const char **mp)
{
	return mp_decode_uint(mp);
}

static uint64_t
decode_number(const char **mp)
{
	uint64_t u_value;
	double value = 0;
	switch (mp_typeof(**mp)) {
		case MP_FLOAT: {
			value = mp_decode_float(mp);
			break;
		}
		case MP_DOUBLE: {
			value = mp_decode_double(mp);
			break;
		}
		case MP_UINT: {
			value = mp_decode_uint(mp);
			break;
		}
		case MP_INT: {
			value = mp_decode_int(mp);
			break;
		}
		default:
			unreachable();
	}
	memcpy(&u_value, &value, sizeof(value));
	return toggle_high_bit(u_value);
}

static uint64_t
decode_integer(const char **mp)
{
	uint64_t u_value;
	int64_t value = 0;
	switch (mp_typeof(**mp)) {
		case MP_UINT: {
			value = mp_decode_uint(mp);
			break;
		}
		case MP_INT: {
			value = mp_decode_int(mp);
			break;
		}
		default:
			unreachable();
	}
	memcpy(&u_value, &value, sizeof(value));
	return toggle_high_bit(u_value);
}

static uint64_t
decode_str(const char **mp)
{
	uint32_t value_len = 0;
	const char* value = mp_decode_str(mp, &value_len);
	return str_to_key_part(value, value_len);
}

static inline uint64_t
mp_decode_to_uint64(const char **mp, enum field_type type)
{
	switch (type) {
		case FIELD_TYPE_UNSIGNED: {
			return decode_uint(mp);
		}
		case FIELD_TYPE_INTEGER: {
			return decode_integer(mp);
		}
		case FIELD_TYPE_NUMBER: {
			return decode_number(mp);
		}
		case FIELD_TYPE_STRING: {
			return decode_str(mp);
		}
		default:
			unreachable();
	}
}

static z_address*
mp_decode_part(const char *mp, uint32_t part_count,
               const struct index_def *index_def, uint32_t even)
{
	uint64_t key_parts[part_count / 2];
    for (uint32_t j = 0; j < part_count; ++j) {
        uint32_t i = j / 2;
        if (j % 2 != even) {
            mp_next(&mp);
            continue;
        }
        if (mp_typeof(*mp) == MP_NIL) {
            if (j % 2 == 0) {
                key_parts[i] = 0;
            } else {
                key_parts[i] = -1ULL;
            }
			mp_next(&mp);
        } else {
			key_parts[i] = mp_decode_to_uint64(&mp, index_def->key_def->parts->type);
        }
    }
	z_address* result = interleave_keys(key_parts, part_count / 2);
    return result;
}

static z_address*
mp_decode_key(const char *mp, uint32_t part_count,
		const struct index_def *index_def)
{
	uint64_t key_parts[part_count];
    for (uint32_t i = 0; i < part_count; ++i) {
		key_parts[i] = mp_decode_to_uint64(&mp, index_def->key_def->parts->type);
    }
	z_address* result = interleave_keys(key_parts, part_count);
    return result;
}

// Extract z-address from tuple
static z_address*
extract_zaddress(struct tuple *tuple, const struct index_def *index_def) {
    uint32_t  key_size;
    const char* key = tuple_extract_key(tuple, index_def->key_def,
    		MULTIKEY_NONE, &key_size);
    mp_decode_array(&key);
    return mp_decode_key(key, index_def->key_def->part_count, index_def);
}

/* {{{ MemtxTree Iterators ****************************************/
struct tree_iterator {
	struct iterator base;
	const struct memtx_zcurve *tree;
	struct index_def *index_def;
	struct memtx_zcurve_iterator tree_iterator;
	enum iterator_type type;
	struct memtx_zcurve_data current;

	z_address* lower_bound;
	z_address* upper_bound;

	/** Memory pool the iterator was allocated from. */
	struct mempool *pool;
};

static void
tree_iterator_free(struct iterator *iterator);

static inline struct tree_iterator *
tree_iterator(struct iterator *it)
{
	assert(it->free == tree_iterator_free);
	return (struct tree_iterator *) it;
}

static void
tree_iterator_free(struct iterator *iterator)
{
	struct tree_iterator *it = tree_iterator(iterator);
	struct tuple *tuple = it->current.tuple;
	if (it->lower_bound != NULL) {
		z_value_free(it->lower_bound);
		it->lower_bound = NULL;
	}
	if (it->upper_bound != NULL) {
		z_value_free(it->upper_bound);
		it->upper_bound = NULL;
	}
	if (tuple != NULL) {
		tuple_unref(tuple);
	}
	mempool_free(it->pool, it);
}

static int
tree_iterator_dummie(struct iterator *iterator, struct tuple **ret)
{
	(void)iterator;
	*ret = NULL;
	return 0;
}

static void
tree_iterator_scroll(struct iterator *iterator, struct tuple **ret) {
	struct tree_iterator *it = tree_iterator(iterator);
	struct memtx_zcurve_data *res =
			memtx_zcurve_iterator_get_elem(it->tree, &it->tree_iterator);

	bool is_relevant = false;
	while (!is_relevant) {
		if (res == NULL || z_value_cmp(res->z_address, it->upper_bound) > 0) {
			iterator->next = tree_iterator_dummie;
			it->current.tuple = NULL;
			*ret = NULL;
			return;
		}

		if (z_value_is_relevant(res->z_address, it->lower_bound, it->upper_bound)) {
			break;
		}

		z_address *next_zvalue = get_next_zvalue(res->z_address,
												 it->lower_bound,
												 it->upper_bound,
												 NULL);
		it->tree_iterator = memtx_zcurve_lower_bound(it->tree, next_zvalue,
													 &is_relevant);
		res = memtx_zcurve_iterator_get_elem(it->tree, &it->tree_iterator);
		z_value_free(next_zvalue);
	}
	*ret = res->tuple;
	tuple_ref(*ret);
	it->current = *res;
}

static int
tree_iterator_next(struct iterator *iterator, struct tuple **ret)
{
	struct tree_iterator *it = tree_iterator(iterator);
	assert(it->current.tuple != NULL && it->current.z_address != NULL);
	struct memtx_zcurve_data *check =
			memtx_zcurve_iterator_get_elem(it->tree, &it->tree_iterator);
	if (check == NULL || !memtx_zcurve_data_identical(check, &it->current)) {
		it->tree_iterator =
				memtx_zcurve_upper_bound_elem(it->tree, it->current,
											NULL);
	} else {
		memtx_zcurve_iterator_next(it->tree, &it->tree_iterator);
	}
	tuple_unref(it->current.tuple);
	tree_iterator_scroll(iterator, ret);
	return 0;
}

static int
tree_iterator_next_equal(struct iterator *iterator, struct tuple **ret)
{
	struct tree_iterator *it = tree_iterator(iterator);
	assert(it->current.tuple != NULL && it->current.z_address != NULL);
	struct memtx_zcurve_data *check =
		memtx_zcurve_iterator_get_elem(it->tree, &it->tree_iterator);
	if (check == NULL || !memtx_zcurve_data_identical(check, &it->current)) {
		it->tree_iterator =
			memtx_zcurve_upper_bound_elem(it->tree, it->current, NULL);
	} else {
		memtx_zcurve_iterator_next(it->tree, &it->tree_iterator);
	}
	tuple_unref(it->current.tuple);
	struct memtx_zcurve_data *res =
		memtx_zcurve_iterator_get_elem(it->tree, &it->tree_iterator);
	/* Use user key def to save a few loops. */
	// TODO: check that value is relevant and not at the end
	if (res == NULL ||
			memtx_zcurve_compare_key(res,
					  it->current.z_address) != 0) {
		iterator->next = tree_iterator_dummie;
		it->current.tuple = NULL;
		it->current.z_address = NULL;
		*ret = NULL;
	} else {
		*ret = res->tuple;
		tuple_ref(*ret);
		it->current = *res;
	}
	return 0;
}

static void
tree_iterator_set_next_method(struct tree_iterator *it)
{
	assert(it->current.tuple != NULL && it->current.z_address != NULL);
	switch (it->type) {
	case ITER_ALL:
		it->base.next = tree_iterator_next;
		break;
	case ITER_EQ:
		it->base.next = tree_iterator_next_equal;
		break;
	case ITER_GE:
		it->base.next = tree_iterator_next;
		break;
	default:
		unreachable();
	}
}

static int
tree_iterator_start(struct iterator *iterator, struct tuple **ret)
{
	*ret = NULL;
	struct tree_iterator *it = tree_iterator(iterator);
	it->base.next = tree_iterator_dummie;
	const struct memtx_zcurve *tree = it->tree;
	enum iterator_type type = it->type;
	bool exact = false;
	assert(it->current.tuple == NULL && it->current.z_address == NULL);

	it->tree_iterator =
		memtx_zcurve_lower_bound(tree, it->lower_bound, &exact);
	if (type == ITER_EQ && !exact)
		return 0;

	tree_iterator_scroll(iterator, ret);
	if (*ret != NULL) {
		tree_iterator_set_next_method(it);
	}

	return 0;
}

/* }}} */

/* {{{ MemtxTree  **********************************************************/

static void
memtx_zcurve_index_free(struct memtx_zcurve_index *index)
{
	memtx_zcurve_destroy(&index->tree);
	free(index->build_array);
	free(index);
}

static void
memtx_zcurve_index_gc_run(struct memtx_gc_task *task, bool *done)
{
	/*
	 * Yield every 1K tuples to keep latency < 0.1 ms.
	 * Yield more often in debug mode.
	 */
#ifdef NDEBUG
	enum { YIELD_LOOPS = 1000 };
#else
	enum { YIELD_LOOPS = 10 };
#endif

	struct memtx_zcurve_index *index = container_of(task,
			struct memtx_zcurve_index, gc_task);
	struct memtx_zcurve *tree = &index->tree;
	struct memtx_zcurve_iterator *itr = &index->gc_iterator;

	unsigned int loops = 0;
	while (!memtx_zcurve_iterator_is_invalid(itr)) {
		struct memtx_zcurve_data *res =
			memtx_zcurve_iterator_get_elem(tree, itr);
		memtx_zcurve_iterator_next(tree, itr);
		tuple_unref(res->tuple);
		if (++loops >= YIELD_LOOPS) {
			*done = false;
			return;
		}
	}
	*done = true;
}

static void
memtx_zcurve_index_gc_free(struct memtx_gc_task *task)
{
	struct memtx_zcurve_index *index = container_of(task,
			struct memtx_zcurve_index, gc_task);
	memtx_zcurve_index_free(index);
}

static const struct memtx_gc_task_vtab memtx_zcurve_index_gc_vtab = {
	.run = memtx_zcurve_index_gc_run,
	.free = memtx_zcurve_index_gc_free,
};

static void
memtx_zcurve_index_destroy(struct index *base)
{
	struct memtx_zcurve_index *index = (struct memtx_zcurve_index *)base;
	struct memtx_engine *memtx = (struct memtx_engine *)base->engine;
	if (base->def->iid == 0) {
		/*
		 * Primary index. We need to free all tuples stored
		 * in the index, which may take a while. Schedule a
		 * background task in order not to block tx thread.
		 */
		index->gc_task.vtab = &memtx_zcurve_index_gc_vtab;
		index->gc_iterator = memtx_zcurve_iterator_first(&index->tree);
		memtx_engine_schedule_gc(memtx, &index->gc_task);
	} else {
		/*
		 * Secondary index. Destruction is fast, no need to
		 * hand over to background fiber.
		 */
		memtx_zcurve_index_free(index);
	}
}

static void
memtx_zcurve_index_update_def(struct index *base)
{
	struct memtx_zcurve_index *index = (struct memtx_zcurve_index *)base;
	struct index_def *def = base->def;
	/*
	 * We use extended key def for non-unique and nullable
	 * indexes. Unique but nullable index can store multiple
	 * NULLs. To correctly compare these NULLs extended key
	 * def must be used. For details @sa tuple_compare.cc.
	 */
	index->tree.arg = def->opts.is_unique && !def->key_def->is_nullable ?
						def->key_def : def->cmp_def;
}

static bool
memtx_zcurve_index_depends_on_pk(struct index *base)
{
	struct index_def *def = base->def;
	/* See comment to memtx_zcurve_index_update_def(). */
	return !def->opts.is_unique || def->key_def->is_nullable;
}

static ssize_t
memtx_zcurve_index_size(struct index *base)
{
	struct memtx_zcurve_index *index = (struct memtx_zcurve_index *)base;
	return memtx_zcurve_size(&index->tree);
}

static ssize_t
memtx_zcurve_index_bsize(struct index *base)
{
	struct memtx_zcurve_index *index = (struct memtx_zcurve_index *)base;
	uint32_t dimension = index->base.def->key_def->part_count;
	ssize_t tree_size = memtx_zcurve_size(&index->tree);
	size_t result = memtx_zcurve_mem_used(&index->tree);
	size_t elem_size = sizeof(z_address) + dimension * sizeof(word_t);
	result += tree_size * elem_size;
	return result;
}

static int
memtx_zcurve_index_random(struct index *base, uint32_t rnd,
		struct tuple **result)
{
	struct memtx_zcurve_index *index = (struct memtx_zcurve_index *)base;
	struct memtx_zcurve_data *res = memtx_zcurve_random(&index->tree, rnd);
	*result = res != NULL ? res->tuple : NULL;
	return 0;
}

static ssize_t
memtx_zcurve_index_count(struct index *base, enum iterator_type type,
		       const char *key, uint32_t part_count)
{
	if (type == ITER_ALL)
		return memtx_zcurve_index_size(base); /* optimization */
	return generic_index_count(base, type, key, part_count);
}

static int
memtx_zcurve_index_get(struct index *base, const char *key,
		     uint32_t part_count, struct tuple **result)
{
	assert(base->def->opts.is_unique &&
	       part_count == base->def->key_def->part_count);
	struct memtx_zcurve_index *index = (struct memtx_zcurve_index *)base;
	z_address* key_data = mp_decode_key(key, part_count, index->base.def);
	struct memtx_zcurve_data *res = memtx_zcurve_find(&index->tree, key_data);
	*result = res != NULL ? res->tuple : NULL;
	z_value_free(key_data);
	return 0;
}

static int
memtx_zcurve_index_replace(struct index *base, struct tuple *old_tuple,
			 struct tuple *new_tuple, enum dup_replace_mode mode,
			 struct tuple **result)
{
	struct memtx_zcurve_index *index = (struct memtx_zcurve_index *)base;
	if (new_tuple) {
		struct memtx_zcurve_data new_data;
		new_data.tuple = new_tuple;
        new_data.z_address = extract_zaddress(new_tuple, index->base.def);
		struct memtx_zcurve_data dup_data;
		dup_data.tuple = NULL;
		dup_data.z_address = NULL;

		/* Try to optimistically replace the new_tuple. */
		int tree_res = memtx_zcurve_insert(&index->tree, new_data,
						 &dup_data);
		if (tree_res) {
			diag_set(OutOfMemory, MEMTX_EXTENT_SIZE,
				 "memtx_zcurve_index", "replace");
			return -1;
		}

		uint32_t errcode = replace_check_dup(old_tuple,
						     dup_data.tuple, mode);
		if (errcode) {
			memtx_zcurve_delete(&index->tree, new_data);
//			z_value_free(new_data.z_address);
			if (dup_data.tuple != NULL)
				memtx_zcurve_insert(&index->tree, dup_data, NULL);
			struct space *sp = space_cache_find(base->def->space_id);
			if (sp != NULL)
				diag_set(ClientError, errcode, base->def->name,
					 space_name(sp));
			return -1;
		}
		if (dup_data.tuple != NULL) {
			*result = dup_data.tuple;
			return 0;
		}
	}
	if (old_tuple) {
		struct memtx_zcurve_data old_data;
		old_data.tuple = old_tuple;
		old_data.z_address = extract_zaddress(old_tuple, index->base.def);
		memtx_zcurve_delete(&index->tree, old_data);
//		z_value_free(old_data.z_address);
	}
	*result = old_tuple;
	return 0;
}

static struct iterator *
memtx_zcurve_index_create_iterator(struct index *base, enum iterator_type type,
				 const char *key, uint32_t part_count)
{
	struct memtx_zcurve_index *index = (struct memtx_zcurve_index *)base;
	struct memtx_engine *memtx = (struct memtx_engine *)base->engine;

	assert(part_count == 0 || key != NULL);
	if (type != ITER_EQ && type != ITER_ALL && type != ITER_GE) {
		diag_set(UnsupportedIndexFeature, base->def,
				 "requested iterator type");
		return NULL;
	}

	if (part_count == 0) {
		/*
		 * If no key is specified, downgrade equality
		 * iterators to a full range.
		 */
		type = ITER_GE;
		key = NULL;
	} else if (base->def->key_def->part_count * 2 == part_count
		&& type != ITER_ALL) {
		/*
		 * If part_count is twice greater than key_def.part_count
		 * set iterator to range query
		 */
		type = ITER_GE;
	}

	struct tree_iterator *it = mempool_alloc(&memtx->zcurve_iterator_pool);
	if (it == NULL) {
		diag_set(OutOfMemory, sizeof(struct tree_iterator),
			 "memtx_zcurve_index", "iterator");
		return NULL;
	}
	iterator_create(&it->base, base);
	it->pool = &memtx->zcurve_iterator_pool;
	it->base.next = tree_iterator_start;
	it->base.free = tree_iterator_free;
	it->type = type;
	it->lower_bound = NULL;
	it->upper_bound = NULL;
	if (part_count == 0 || type == ITER_ALL) {
		it->lower_bound = zeros(base->def->key_def->part_count);
		it->upper_bound = ones(base->def->key_def->part_count);
	} else if (base->def->key_def->part_count == part_count) {
		it->lower_bound = mp_decode_key(key, part_count, index->base.def);
		it->upper_bound = ones(part_count);
	} else if (base->def->key_def->part_count * 2 == part_count) {
		it->lower_bound = mp_decode_part(key, part_count, index->base.def, 0);
		it->upper_bound = mp_decode_part(key, part_count, index->base.def, 1);
	} else {
		unreachable();
	}
	it->index_def = base->def;
	it->tree = &index->tree;
	it->tree_iterator = memtx_zcurve_invalid_iterator();
	it->current.tuple = NULL;
	it->current.z_address = NULL;
	return (struct iterator *)it;
}

static void
memtx_zcurve_index_begin_build(struct index *base)
{
	struct memtx_zcurve_index *index = (struct memtx_zcurve_index *)base;
	assert(memtx_zcurve_size(&index->tree) == 0);
	(void)index;
}

static int
memtx_zcurve_index_reserve(struct index *base, uint32_t size_hint)
{
	struct memtx_zcurve_index *index = (struct memtx_zcurve_index *)base;
	if (size_hint < index->build_array_alloc_size)
		return 0;
	struct memtx_zcurve_data *tmp =
		realloc(index->build_array, size_hint * sizeof(*tmp));
	if (tmp == NULL) {
		diag_set(OutOfMemory, size_hint * sizeof(*tmp),
			 "memtx_zcurve_index", "reserve");
		return -1;
	}
	index->build_array = tmp;
	index->build_array_alloc_size = size_hint;
	return 0;
}

static int
memtx_zcurve_index_build_next(struct index *base, struct tuple *tuple)
{
	struct memtx_zcurve_index *index = (struct memtx_zcurve_index *)base;
	if (index->build_array == NULL) {
		index->build_array = malloc(MEMTX_EXTENT_SIZE);
		if (index->build_array == NULL) {
			diag_set(OutOfMemory, MEMTX_EXTENT_SIZE,
				 "memtx_zcurve_index", "build_next");
			return -1;
		}
		index->build_array_alloc_size =
			MEMTX_EXTENT_SIZE / sizeof(index->build_array[0]);
	}
	assert(index->build_array_size <= index->build_array_alloc_size);
	if (index->build_array_size == index->build_array_alloc_size) {
		index->build_array_alloc_size = index->build_array_alloc_size +
					index->build_array_alloc_size / 2;
		struct memtx_zcurve_data *tmp =
			realloc(index->build_array,
				index->build_array_alloc_size * sizeof(*tmp));
		if (tmp == NULL) {
			diag_set(OutOfMemory, index->build_array_alloc_size *
				 sizeof(*tmp), "memtx_zcurve_index", "build_next");
			return -1;
		}
		index->build_array = tmp;
	}
	struct memtx_zcurve_data *elem =
		&index->build_array[index->build_array_size++];
	elem->tuple = tuple;
	elem->z_address = extract_zaddress(tuple, index->base.def);
	return 0;
}

static void
memtx_zcurve_index_end_build(struct index *base)
{
	struct memtx_zcurve_index *index = (struct memtx_zcurve_index *)base;
	qsort(index->build_array, index->build_array_size,
		  sizeof(index->build_array[0]), memtx_zcurve_qcompare);
	memtx_zcurve_build(&index->tree, index->build_array,
			 index->build_array_size);

	free(index->build_array);
	index->build_array = NULL;
	index->build_array_size = 0;
	index->build_array_alloc_size = 0;
}

struct tree_snapshot_iterator {
	struct snapshot_iterator base;
	struct memtx_zcurve *tree;
	struct memtx_zcurve_iterator tree_iterator;
};

static void
tree_snapshot_iterator_free(struct snapshot_iterator *iterator)
{
	assert(iterator->free == tree_snapshot_iterator_free);
	struct tree_snapshot_iterator *it =
		(struct tree_snapshot_iterator *)iterator;
	struct memtx_zcurve *tree = (struct memtx_zcurve *)it->tree;
	memtx_zcurve_iterator_destroy(tree, &it->tree_iterator);
	free(iterator);
}

static const char *
tree_snapshot_iterator_next(struct snapshot_iterator *iterator, uint32_t *size)
{
	assert(iterator->free == tree_snapshot_iterator_free);
	struct tree_snapshot_iterator *it =
		(struct tree_snapshot_iterator *)iterator;
	struct memtx_zcurve_data *res =
		memtx_zcurve_iterator_get_elem(it->tree, &it->tree_iterator);
	if (res == NULL)
		return NULL;
	memtx_zcurve_iterator_next(it->tree, &it->tree_iterator);
	return tuple_data_range(res->tuple, size);
}

/**
 * Create an ALL iterator with personal read view so further
 * index modifications will not affect the iteration results.
 * Must be destroyed by iterator->free after usage.
 */
static struct snapshot_iterator *
memtx_zcurve_index_create_snapshot_iterator(struct index *base)
{
	struct memtx_zcurve_index *index = (struct memtx_zcurve_index *)base;
	struct tree_snapshot_iterator *it = (struct tree_snapshot_iterator *)
		calloc(1, sizeof(*it));
	if (it == NULL) {
		diag_set(OutOfMemory, sizeof(struct tree_snapshot_iterator),
			 "memtx_zcurve_index", "create_snapshot_iterator");
		return NULL;
	}

	it->base.free = tree_snapshot_iterator_free;
	it->base.next = tree_snapshot_iterator_next;
	it->tree = &index->tree;
	it->tree_iterator = memtx_zcurve_iterator_first(&index->tree);
	memtx_zcurve_iterator_freeze(&index->tree, &it->tree_iterator);
	return (struct snapshot_iterator *) it;
}

static const struct index_vtab memtx_zcurve_index_vtab = {
	/* .destroy = */ memtx_zcurve_index_destroy,
	/* .commit_create = */ generic_index_commit_create,
	/* .abort_create = */ generic_index_abort_create,
	/* .commit_modify = */ generic_index_commit_modify,
	/* .commit_drop = */ generic_index_commit_drop,
	/* .update_def = */ memtx_zcurve_index_update_def,
	/* .depends_on_pk = */ memtx_zcurve_index_depends_on_pk,
	/* .def_change_requires_rebuild = */
		memtx_index_def_change_requires_rebuild,
	/* .size = */ memtx_zcurve_index_size,
	/* .bsize = */ memtx_zcurve_index_bsize,
	/* .min = */ generic_index_min,
	/* .max = */ generic_index_max,
	/* .random = */ memtx_zcurve_index_random,
	/* .count = */ memtx_zcurve_index_count,
	/* .get = */ memtx_zcurve_index_get,
	/* .replace = */ memtx_zcurve_index_replace,
	/* .create_iterator = */ memtx_zcurve_index_create_iterator,
	/* .create_snapshot_iterator = */
		memtx_zcurve_index_create_snapshot_iterator,
	/* .stat = */ generic_index_stat,
	/* .compact = */ generic_index_compact,
	/* .reset_stat = */ generic_index_reset_stat,
	/* .begin_build = */ memtx_zcurve_index_begin_build,
	/* .reserve = */ memtx_zcurve_index_reserve,
	/* .build_next = */ memtx_zcurve_index_build_next,
	/* .end_build = */ memtx_zcurve_index_end_build,
};

struct index *
memtx_zcurve_index_new(struct memtx_engine *memtx, struct index_def *def)
{
	if (def->key_def->part_count < 1 ||
	        def->key_def->part_count > ZCURVE_MAX_DIMENSION) {
		diag_set(UnsupportedIndexFeature, def,
				 tt_sprintf("dimension (%lld): must belong to "
							"range [%u, %u]", def->key_def->part_count,
							1, ZCURVE_MAX_DIMENSION));
		return NULL;
	}

	if (!mempool_is_initialized(&memtx->zcurve_iterator_pool)) {
		mempool_create(&memtx->zcurve_iterator_pool, cord_slab_cache(),
					   sizeof(struct tree_iterator));
	}
	struct memtx_zcurve_index *index =
		(struct memtx_zcurve_index *)calloc(1, sizeof(*index));
	if (index == NULL) {
		diag_set(OutOfMemory, sizeof(*index),
			 "malloc", "struct memtx_zcurve_index");
		return NULL;
	}
	if (index_create(&index->base, (struct engine *)memtx,
			 &memtx_zcurve_index_vtab, def) != 0) {
		free(index);
		return NULL;
	}

	/* See comment to memtx_zcurve_index_update_def(). */
	struct key_def *cmp_def = def->opts.is_unique &&
			!def->key_def->is_nullable ?
			index->base.def->key_def : index->base.def->cmp_def;
	memtx_zcurve_create(&index->tree, cmp_def, memtx_index_extent_alloc,
			  memtx_index_extent_free, memtx);
	return &index->base;
}
