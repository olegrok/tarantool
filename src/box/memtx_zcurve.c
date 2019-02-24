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
#include "memtx_zcurve.h"
#include "memtx_engine.h"
#include "space.h"
#include "schema.h" /* space_cache_find() */
#include "errinj.h"
#include "memory.h"
#include "fiber.h"
#include "tuple.h"
#include <third_party/qsort_arg.h>
#include <small/mempool.h>
#include <salad/bit_array.h>

/* {{{ Utilities. *************************************************/

static int
memtx_zcurve_qcompare(const void* a, const void *b, void *c)
{
	return tuple_compare(*(struct tuple **)a,
						 *(struct tuple **)b, (struct key_def *)c);
}

static inline int
mp_decode_num(const char **data, uint32_t fieldno, double *ret)
{
    if (mp_read_double(data, ret) != 0) {
        diag_set(ClientError, ER_FIELD_TYPE,
                 int2str(fieldno + TUPLE_INDEX_BASE),
                 field_type_strs[FIELD_TYPE_NUMBER]);
        return -1;
    }
    return 0;
}

static BIT_ARRAY*
interleave_keys(BIT_ARRAY** const keys, size_t size) {
	const size_t bits_in_key = 64;
	BIT_ARRAY* result = bit_array_create(size * bits_in_key);
	for (size_t i = 0; i < size; i++) {
		for (size_t j = 0; j < bits_in_key; j++) {
			if (bit_array_get_bit(keys[i], j) == 1) {
				bit_array_set_bit(result, size * j + i);
			}
		}
	}
	return result;
}

static BIT_ARRAY*
extract_zaddress(const struct tuple *tuple, const struct index_def *index_def) {
    const uint32_t part_count = index_def->key_def->part_count;
	BIT_ARRAY* key_parts[part_count];
    for (uint32_t i = 0; i < part_count; i++) {
        const char *elems = tuple_field_by_part(tuple,
                                                &index_def->key_def->parts[i]);
        key_parts[i] = bit_array_create(sizeof(int64_t) * 8);
        switch(mp_typeof(*elems)) {
            case MP_DOUBLE: {
                double value;
                // TODO: add check
                mp_decode_num(&elems, i, &value);
                bit_array_set_word64(key_parts[i], 0, *(uint64_t *) (&value));
                bit_array_print(key_parts[i], stdout);
				printf("MP_DOUBLE %lg\n", value);
                break;
            }
            case MP_INT:
                printf("MP_INT\n");
                break;
            case MP_UINT: {
                uint64_t value = mp_decode_uint(&elems);
				bit_array_set_word64(key_parts[i], 0, value);
				bit_array_print(key_parts[i], stdout);
                printf("MP_UINT %lu\n", value);
                break;
            }
            default:
                printf("default\n");
        }
    }

    BIT_ARRAY* result = interleave_keys(key_parts, part_count);
	bit_array_print(result, stdout);
	for (uint32_t i = 0; i < part_count; i++) {
		bit_array_free(key_parts[i]);
	}
	return result;
}

static BIT_ARRAY*
mp_decode_key(const char *mp, uint32_t part_count) {
    (void)mp;
    (void)part_count;
    printf("part_count %d\n", part_count);
    double val[part_count];
    BIT_ARRAY* key_parts[part_count];
    for (uint32_t i = 0; i < part_count; ++i) {
        if (mp_decode_num(&mp, i, &val[i]) < 0) {
            printf("Error\n");
        } else {
            printf("Result: %g\n", val[i]);
            key_parts[i] = bit_array_create(sizeof(int64_t) * 8);
            printf("sizeof %ld\n", sizeof(int64_t) * 8);
            bit_array_set_word64(key_parts[i], 0, *(uint64_t*)(&val[i]));
            bit_array_print(key_parts[i], stdout);
        }
    }
    BIT_ARRAY* result = interleave_keys(key_parts, part_count);
    for (uint32_t i = 0; i < part_count; ++i) {
        bit_array_free(key_parts[i]);
    }
    bit_array_print(result, stdout);
    return result;
}

/* {{{ MemtxTree Iterators ****************************************/
struct tree_iterator {
	struct iterator base;
	const struct memtx_zcurve *tree;
	struct index_def *index_def;
	struct memtx_zcurve_iterator tree_iterator;
	enum iterator_type type;
	struct memtx_zcurve_key_data key_data;
	struct memtx_zcurve_element *current_element;
	/** Memory pool the iterator was allocated from. */
	struct mempool *pool;
};

static_assert(sizeof(struct tree_iterator) <= MEMTX_ITERATOR_SIZE,
              "sizeof(struct tree_iterator) must be less than or equal "
              "to MEMTX_ITERATOR_SIZE");

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
	if (it->current_element != NULL) {
		bit_array_free(it->current_element->z_address);
		tuple_unref(it->current_element->tuple);
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

static int
tree_iterator_next(struct iterator *iterator, struct tuple **ret)
{
	struct memtx_zcurve_element **res;
	struct tree_iterator *it = tree_iterator(iterator);
	assert(it->current_element != NULL);
	struct memtx_zcurve_element **check = memtx_zcurve_iterator_get_elem(it->tree, &it->tree_iterator);
	if (check == NULL || *check != it->current_element)
		it->tree_iterator =
				memtx_zcurve_upper_bound_elem(it->tree, it->current_element, NULL);
	else
		memtx_zcurve_iterator_next(it->tree, &it->tree_iterator);
	tuple_unref(it->current_element->tuple);
	bit_array_free(it->current_element->z_address);
	it->current_element = NULL;
	res = memtx_zcurve_iterator_get_elem(it->tree, &it->tree_iterator);
	if (res == NULL) {
		iterator->next = tree_iterator_dummie;
		*ret = NULL;
	} else {
		it->current_element = *res;
		*ret = it->current_element->tuple;
		tuple_ref(it->current_element->tuple);
	}
	return 0;
}

static int
tree_iterator_prev(struct iterator *iterator, struct tuple **ret)
{
	struct tree_iterator *it = tree_iterator(iterator);
	assert(it->current_element != NULL);
	struct memtx_zcurve_element **check = memtx_zcurve_iterator_get_elem(it->tree, &it->tree_iterator);
	if (check == NULL || *check != it->current_element)
		it->tree_iterator =
				memtx_zcurve_lower_bound_elem(it->tree, it->current_element,
											NULL);
	memtx_zcurve_iterator_prev(it->tree, &it->tree_iterator);
	tuple_unref(it->current_element->tuple);
	bit_array_free(it->current_element->z_address);
	it->current_element = NULL;
	struct memtx_zcurve_element **res = memtx_zcurve_iterator_get_elem(it->tree, &it->tree_iterator);
	if (!res) {
		iterator->next = tree_iterator_dummie;
		*ret = NULL;
	} else {
		*ret = it->current_element->tuple = (*res)->tuple;
		tuple_ref(it->current_element->tuple);
	}
	return 0;
}

static int
tree_iterator_next_equal(struct iterator *iterator, struct tuple **ret)
{
	struct tree_iterator *it = tree_iterator(iterator);
	assert(it->current_element != NULL);
	struct memtx_zcurve_element **check = memtx_zcurve_iterator_get_elem(it->tree,
														&it->tree_iterator);
	if (check == NULL || *check != it->current_element)
		it->tree_iterator =
				memtx_zcurve_upper_bound_elem(it->tree, it->current_element,
											NULL);
	else
		memtx_zcurve_iterator_next(it->tree, &it->tree_iterator);
	tuple_unref(it->current_element->tuple);
	bit_array_free(it->current_element->z_address);
	it->current_element = NULL;
	struct memtx_zcurve_element **res = memtx_zcurve_iterator_get_elem(it->tree,
													  &it->tree_iterator);
	/* Use user key def to save a few loops. */
	printf("bit_array_print(it->key_data.key,stdout)\n");
	fflush(stdout);
	bit_array_print(it->key_data.key,stdout);
	if (!res || memtx_zcurve_compare_key(*res, &it->key_data,
									   it->index_def->key_def) != 0) {
		iterator->next = tree_iterator_dummie;
		*ret = NULL;
	} else {
		*ret = it->current_element->tuple = (*res)->tuple;
		tuple_ref(it->current_element->tuple);
	}
	return 0;
}

static int
tree_iterator_prev_equal(struct iterator *iterator, struct tuple **ret)
{
	struct tree_iterator *it = tree_iterator(iterator);
	assert(it->current_element != NULL);
	struct memtx_zcurve_element **check = memtx_zcurve_iterator_get_elem(it->tree,
														&it->tree_iterator);
	if (check == NULL || *check != it->current_element)
		it->tree_iterator =
				memtx_zcurve_lower_bound_elem(it->tree, it->current_element,
											NULL);
	memtx_zcurve_iterator_prev(it->tree, &it->tree_iterator);
	tuple_unref(it->current_element->tuple);
	it->current_element = NULL;
	struct memtx_zcurve_element **res = memtx_zcurve_iterator_get_elem(it->tree,
													  &it->tree_iterator);
	/* Use user key def to save a few loops. */
	if (!res || memtx_zcurve_compare_key(*res, &it->key_data,
									   it->index_def->key_def) != 0) {
		iterator->next = tree_iterator_dummie;
		*ret = NULL;
	} else {
		*ret = it->current_element->tuple = (*res)->tuple;
		tuple_ref(it->current_element->tuple);
	}
	return 0;
}

static void
tree_iterator_set_next_method(struct tree_iterator *it)
{
	assert(it->current_element != NULL);
	switch (it->type) {
		case ITER_EQ:
			it->base.next = tree_iterator_next_equal;
			break;
		case ITER_REQ:
			it->base.next = tree_iterator_prev_equal;
			break;
		case ITER_ALL:
			it->base.next = tree_iterator_next;
			break;
		case ITER_LT:
		case ITER_LE:
			it->base.next = tree_iterator_prev;
			break;
		case ITER_GE:
		case ITER_GT:
			it->base.next = tree_iterator_next;
			break;
		default:
			/* The type was checked in initIterator */
			assert(false);
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
	assert(it->current_element == NULL);
	if (it->key_data.key == 0) {
		if (iterator_type_is_reverse(it->type))
			it->tree_iterator = memtx_zcurve_iterator_last(tree);
		else
			it->tree_iterator = memtx_zcurve_iterator_first(tree);
	} else {
		if (type == ITER_ALL || type == ITER_EQ ||
			type == ITER_GE || type == ITER_LT) {
			it->tree_iterator =
					memtx_zcurve_lower_bound(tree, &it->key_data,
										   &exact);
			if (type == ITER_EQ && !exact)
				return 0;
		} else { // ITER_GT, ITER_REQ, ITER_LE
			it->tree_iterator =
					memtx_zcurve_upper_bound(tree, &it->key_data,
										   &exact);
			if (type == ITER_REQ && !exact)
				return 0;
		}
		if (iterator_type_is_reverse(type)) {
			/*
			 * Because of limitations of tree search API we use
			 * lower_bound for LT search and upper_bound for LE
			 * and REQ searches. Thus we found position to the
			 * right of the target one. Let's make a step to the
			 * left to reach target position.
			 * If we found an invalid iterator all the elements in
			 * the tree are less (less or equal) to the key, and
			 * iterator_next call will convert the iterator to the
			 * last position in the tree, that's what we need.
			 */
			memtx_zcurve_iterator_prev(it->tree, &it->tree_iterator);
		}
	}

	struct memtx_zcurve_element **res = memtx_zcurve_iterator_get_elem(it->tree,
													  &it->tree_iterator);
	if (!res)
		return 0;
	printf("tree_iterator_start\n");
	bit_array_print((*res)->z_address, stdout);
	printf("(*res)->tuple %p\n", (*res)->tuple);
	fflush(stdout);
	printf("(*res)->tuple content %s", tuple_str((*res)->tuple));
	fflush(stdout);
	it->current_element = *res;
	*ret = it->current_element->tuple;
	tuple_ref(it->current_element->tuple);
	tree_iterator_set_next_method(it);
	return 0;
}

/* }}} */

/* {{{ MemtxTree  **********************************************************/

/**
 * Return the key def to use for comparing tuples stored
 * in the given tree index.
 *
 * We use extended key def for non-unique and nullable
 * indexes. Unique but nullable index can store multiple
 * NULLs. To correctly compare these NULLs extended key
 * def must be used. For details @sa tuple_compare.cc.
 */
static struct key_def *
memtx_zcurve_index_cmp_def(struct memtx_zcurve_index *index)
{
	struct index_def *def = index->base.def;
	return def->opts.is_unique && !def->key_def->is_nullable ?
		   def->key_def : def->cmp_def;
}

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
		struct memtx_zcurve_element *elem = *memtx_zcurve_iterator_get_elem(tree, itr);
		memtx_zcurve_iterator_next(tree, itr);
		tuple_unref(elem->tuple);
		bit_array_free(elem->z_address);
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
	index->tree.arg = memtx_zcurve_index_cmp_def(index);
}

static bool
memtx_zcurve_index_depends_on_pk(struct index *base)
{
	struct index_def *def = base->def;
	/* See comment to memtx_zcurve_index_cmp_def(). */
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
	return memtx_zcurve_mem_used(&index->tree);
}

static int
memtx_zcurve_index_random(struct index *base, uint32_t rnd, struct tuple **result)
{
	struct memtx_zcurve_index *index = (struct memtx_zcurve_index *)base;
	struct memtx_zcurve_element **res = memtx_zcurve_random(&index->tree, rnd);
	*result = res != NULL ? (*res)->tuple : NULL;
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
//	struct memtx_zcurve_index *index = (struct memtx_zcurve_index *)base;
//	struct memtx_zcurve_key_data key_data;
	// FIXME: Check that it's work
    BIT_ARRAY* decoded_key = mp_decode_key(key, part_count);
    (void)decoded_key;
    *result = NULL;

//	key_data.key = decoded_key;
//	key_data.part_count = part_count;
//	struct memtx_zcurve_element **res = memtx_zcurve_find(&index->tree, &key_data);
//	*result = res != NULL ? (*res)->tuple : NULL;
	return 0;
}

static int
memtx_zcurve_index_replace(struct index *base, struct tuple *old_tuple,
						 struct tuple *new_tuple, enum dup_replace_mode mode,
						 struct tuple **result)
{
	struct memtx_zcurve_index *index = (struct memtx_zcurve_index *)base;
	if (new_tuple) {
		struct memtx_zcurve_element *dup_elem = NULL;
		struct memtx_zcurve_element *new_elem = malloc(sizeof(struct memtx_zcurve_element));
		new_elem->tuple = new_tuple;
		new_elem->z_address = extract_zaddress(new_tuple, index->base.def);

		printf("REPLACE %p\n", new_elem->z_address);
		bit_array_print(new_elem->z_address, stdout);
		/* Try to optimistically replace the new_tuple. */
		int tree_res = memtx_zcurve_insert(&index->tree,
										 new_elem, &dup_elem);
		if (tree_res) {
			diag_set(OutOfMemory, MEMTX_EXTENT_SIZE,
					 "memtx_zcurve_index", "replace");
			return -1;
		}

		uint32_t errcode = replace_check_dup(old_tuple,
											 dup_elem != NULL ? dup_elem->tuple : NULL,
											 mode);
		if (errcode) {
			memtx_zcurve_delete(&index->tree, new_elem);
			if (dup_elem)
				memtx_zcurve_insert(&index->tree, dup_elem, 0);
			struct space *sp = space_cache_find(base->def->space_id);
			if (sp != NULL)
				diag_set(ClientError, errcode, base->def->name,
						 space_name(sp));
			return -1;
		}
		if (dup_elem) {
			*result = dup_elem->tuple;
			return 0;
		}
	}
	if (old_tuple) {
		struct memtx_zcurve_element old_elem = {
				.tuple = old_tuple,
				.z_address = extract_zaddress(old_tuple, index->base.def),
		};
		memtx_zcurve_delete(&index->tree, &old_elem);
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
	if (type > ITER_GT) {
		diag_set(UnsupportedIndexFeature, base->def,
				 "requested iterator type");
		return NULL;
	}

	if (part_count == 0) {
		/*
		 * If no key is specified, downgrade equality
		 * iterators to a full range.
		 */
		type = iterator_type_is_reverse(type) ? ITER_LE : ITER_GE;
		key = NULL;
	}

	struct tree_iterator *it = mempool_alloc(&memtx->iterator_pool);
	if (it == NULL) {
		diag_set(OutOfMemory, sizeof(struct tree_iterator),
				 "memtx_zcurve_index", "iterator");
		return NULL;
	}
	iterator_create(&it->base, base);
	it->pool = &memtx->iterator_pool;
	it->base.next = tree_iterator_start;
	it->base.free = tree_iterator_free;
	it->type = type;
	// FIXME: Check that it works
	it->key_data.key = mp_decode_key(key, part_count);
	it->key_data.part_count = part_count;
	it->index_def = base->def;
	it->tree = &index->tree;
	it->tree_iterator = memtx_zcurve_invalid_iterator();
	it->current_element = NULL;
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
	struct memtx_zcurve_element **tmp = (struct memtx_zcurve_element **)realloc(index->build_array,
												  size_hint * sizeof(*tmp));
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
		index->build_array = (struct memtx_zcurve_element **)malloc(MEMTX_EXTENT_SIZE);
		if (index->build_array == NULL) {
			diag_set(OutOfMemory, MEMTX_EXTENT_SIZE,
					 "memtx_zcurve_index", "build_next");
			return -1;
		}
		index->build_array_alloc_size =
				MEMTX_EXTENT_SIZE / sizeof(struct tuple*);
	}
	assert(index->build_array_size <= index->build_array_alloc_size);
	if (index->build_array_size == index->build_array_alloc_size) {
		index->build_array_alloc_size = index->build_array_alloc_size +
										index->build_array_alloc_size / 2;
		struct memtx_zcurve_element **tmp = (struct memtx_zcurve_element **)
				realloc(index->build_array,
						index->build_array_alloc_size * sizeof(*tmp));
		if (tmp == NULL) {
			diag_set(OutOfMemory, index->build_array_alloc_size *
								  sizeof(*tmp), "memtx_zcurve_index", "build_next");
			return -1;
		}
		index->build_array = tmp;
	}
	struct memtx_zcurve_element* elem = (struct memtx_zcurve_element*)malloc(sizeof(struct memtx_zcurve_element));
	elem->tuple = tuple;
	elem->z_address = extract_zaddress(tuple, index->base.def);
	index->build_array[index->build_array_size++] = elem;
	return 0;
}

static void
memtx_zcurve_index_end_build(struct index *base)
{
	struct memtx_zcurve_index *index = (struct memtx_zcurve_index *)base;
	struct key_def *cmp_def = memtx_zcurve_index_cmp_def(index);
	qsort_arg(index->build_array, index->build_array_size,
			  // FIXME: change to memtx_zcurve_element
			  sizeof(struct tuple *),
			  memtx_zcurve_qcompare, cmp_def);
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
	struct memtx_zcurve_element **res = memtx_zcurve_iterator_get_elem(it->tree,
													  &it->tree_iterator);
	if (res == NULL)
		return NULL;
	memtx_zcurve_iterator_next(it->tree, &it->tree_iterator);
	return tuple_data_range((*res)->tuple, size);
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

	struct key_def *cmp_def = memtx_zcurve_index_cmp_def(index);
	memtx_zcurve_create(&index->tree, cmp_def, memtx_index_extent_alloc,
					  memtx_index_extent_free, memtx);
	return &index->base;
}
