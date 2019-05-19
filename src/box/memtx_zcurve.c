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
#include <salad/bit_array.h>

#include "memtx_zcurve.h"
#include "memtx_engine.h"
#include "space.h"
#include "schema.h" /* space_cache_find() */
#include "errinj.h"
#include "memory.h"
#include "fiber.h"
#include "tuple.h"
#include <third_party/qsort_arg.h>

/**
 * Struct that is used as a key in BPS tree definition.
 */
struct memtx_zcurve_key_data {
	/** Sequence of msgpacked search fields. */
    const char *key;
	/** Number of msgpacked search fields. */
	uint32_t part_count;
    /** */
	BIT_ARRAY *z_address;
	BIT_ARRAY *z_address_end;
};

/**
 * Struct that is used as a elem in BPS tree definition.
 */
struct memtx_zcurve_data {
    /** Z-address. Read here: https://en.wikipedia.org/wiki/Z-order_curve */
    BIT_ARRAY *z_address;
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
                         const struct memtx_zcurve_key_data *key_data,
                         struct key_def *def)
{
    (void)def;
    assert(element->tuple != NULL && element->z_address != NULL);
//    assert(key_data->key != NULL);
    return bit_array_cmp(element->z_address, key_data->z_address);
}

static inline int
memtx_zcurve_elem_compare(const struct memtx_zcurve_data *elem_a,
                          const struct memtx_zcurve_data *elem_b,
                          struct key_def *key_def)
{
    (void)key_def;
    return bit_array_cmp(elem_a->z_address, elem_b->z_address);
}

#define BPS_TREE_NAME memtx_zcurve
#define BPS_TREE_BLOCK_SIZE (512)
#define BPS_TREE_EXTENT_SIZE MEMTX_EXTENT_SIZE
#define BPS_TREE_COMPARE(a, b, arg) memtx_zcurve_elem_compare(&a, &b, arg)
#define BPS_TREE_COMPARE_KEY(a, b, arg) memtx_zcurve_compare_key(&a, b, arg)
#define BPS_TREE_IDENTICAL(a, b) memtx_zcurve_data_identical(&a, &b)
#define bps_tree_elem_t struct memtx_zcurve_data
#define bps_tree_key_t struct memtx_zcurve_key_data *
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

static inline struct key_def *
memtx_zcurve_cmp_def(struct memtx_zcurve *tree)
{
	return tree->arg;
}

#define KEY_SIZE_IN_BITS (64lu)

static int
memtx_zcurve_qcompare(const void* a, const void *b, void *c)
{
	const struct memtx_zcurve_data *data_a = a;
	const struct memtx_zcurve_data *data_b = b;
	(void)c;
	assert(data_a != NULL && data_a->z_address != NULL);
	assert(data_b != NULL && data_b->z_address != NULL);
	return bit_array_cmp(data_a->z_address, data_b->z_address);
}

static BIT_ARRAY*
zeros(uint32_t part_count) {
    BIT_ARRAY* result = bit_array_create(part_count * KEY_SIZE_IN_BITS);
    bit_array_clear_all(result);
    return result;
}

static BIT_ARRAY*
ones(uint32_t part_count) {
    BIT_ARRAY* result = bit_array_create(part_count * KEY_SIZE_IN_BITS);
    bit_array_set_all(result);
    return result;
}

// Provided a minimum Z-address, a maximum Z-address, and a test Z-address,
// the isRelevant function tells us whether the test address falls within
// the query rectangle created by the minimum and maximum Z-addresses.
static bool
is_relevant(BIT_ARRAY* lower_bound, BIT_ARRAY* upper_bound,
		BIT_ARRAY* test) {
    size_t len = bit_array_length(lower_bound);
    assert(len == bit_array_length(upper_bound));
    assert(len == bit_array_length(test));

    uint32_t dim = len / KEY_SIZE_IN_BITS;
	// TODO: move grid to key_def
    BIT_ARRAY* grid[dim];
	for (uint32_t i = 0; i < dim; ++i) {
		grid[i] = bit_array_create(len);
		for (int j = 0; j < 64; ++j) {
			bit_array_set_bit(grid[i], dim * j);
		}
		bit_array_shift_left(grid[i], i, 0);
	}

	BIT_ARRAY *low, *high, *test_grid;
	low = bit_array_create(len);
	high = bit_array_create(len);
	test_grid = bit_array_create(len);
	bool result = true;

	for (uint32_t i = 0; i < dim; ++i) {
		bit_array_and(low, grid[i], lower_bound);
		bit_array_and(test_grid, grid[i], test);
		bit_array_and(high, grid[i], upper_bound);
		if (bit_array_cmp(low, test_grid) > 0 || bit_array_cmp(test_grid, high) > 0) {
			result = false;
			break;
		}
	}

	bit_array_free(low);
	bit_array_free(high);
	bit_array_free(test_grid);
	for (uint32_t i = 0; i < dim; ++i) {
		bit_array_free(grid[i]);
	}
	return result;
}

size_t bit_position(size_t index_dim, uint32_t dim, uint8_t step) {
	return index_dim * step + dim;
}

uint32_t get_dim(uint32_t index_dim, size_t bit_position) {
	return bit_position % index_dim;
}

uint8_t get_step(uint32_t index_dim, size_t bit_position) {
	return bit_position / index_dim;
}

BIT_ARRAY* get_next_zvalue(BIT_ARRAY* z_value, BIT_ARRAY* lower_bound,
		BIT_ARRAY* upper_bound) {
	const size_t key_len = bit_array_length(z_value);
	assert(key_len == bit_array_length(lower_bound));
	assert(key_len == bit_array_length(upper_bound));
	const uint32_t index_dim = key_len / KEY_SIZE_IN_BITS;

	BIT_ARRAY* result = bit_array_clone(z_value);
	bit_array_add_uint64(result, 1);

	int8_t flag[index_dim], out_step[index_dim];
	int32_t save_min[index_dim], save_max[index_dim];

	for (uint32_t i = 0; i < index_dim; ++i) {
		flag[i] = 0;
		out_step[i] = INT8_MIN;
		save_min[i] = -1;
		save_max[i] = -1;
	}

	size_t bp = key_len;
	do {
		bp--;
		uint32_t dim = get_dim(index_dim, bp);
		uint8_t step = get_step(index_dim, bp);

		if (bit_array_get_bit(result, bp) > bit_array_get_bit(lower_bound, bp)) {
			if (save_min[dim] == -1) {
				save_min[dim] = step;
			}
		} else if (bit_array_get_bit(result, bp) < bit_array_get_bit(lower_bound, bp)) {
			if (flag[dim] == 0 && save_min[dim] == -1) {
				out_step[dim] = step;
				flag[dim] = -1;
			}
		}

		if (bit_array_get_bit(result, bp) < bit_array_get_bit(upper_bound, bp)) {
			if (save_max[dim] == -1) {
				save_max[dim] = step;
			}
		} else if (bit_array_get_bit(result, bp) > bit_array_get_bit(upper_bound, bp)) {
			if (flag[dim] == 0 && save_max[dim] == -1) {
				out_step[dim] = step;
				flag[dim] = 1;
			}
		}
	} while (bp > 0);

	// Next intersection point
	bool is_nip = true;
	for (uint32_t dim = 0; dim < index_dim; ++dim) {
		if (flag[dim] != 0) {
			is_nip = false;
		}
	}
	if (is_nip) {
		return result;
	}

	uint32_t max_dim = 0;
	int8_t max_out_step = -1;

	uint32_t i = index_dim;
	do {
		i--;
		if (max_out_step < out_step[i]) {
			max_out_step = out_step[i];
			max_dim = i;
		}
	} while (i != 0);

	size_t max_bp = bit_position(index_dim, max_dim, max_out_step);
	if (flag[max_dim] == 1) {
		for (size_t new_bp = max_bp + 1; new_bp < key_len; ++new_bp) {
			if (get_step(index_dim, new_bp) <= save_max[get_dim(index_dim, new_bp)] &&
				bit_array_get_bit(result, new_bp) == 0) {
				max_bp = new_bp;
				break;
			}
		}
		// some attributes have to be updated for further processing
		uint32_t max_bp_dim = get_dim(index_dim, max_bp);
		save_min[max_bp_dim] = get_step(index_dim, max_bp);
		flag[max_bp_dim] = 0;
	}

	for (uint32_t dim = 0; dim < index_dim; ++dim) {
		if (flag[dim] >= 0) {
			// nip has not fallen below the minimum in dim
			if (max_bp <= bit_position(index_dim, dim, save_min[dim])) {
				// set all bits in dimension dim with
				// bit position < max_bp to 0 because nip would not surely get below
				// the lower_bound
				for (size_t bit_pos = dim; bit_pos < index_dim * KEY_SIZE_IN_BITS - 1;
						bit_pos += index_dim) {
					if (bit_pos >= max_bp) {
						break;
					}
					bit_array_clear_bit(result, bit_pos);
				}
			} else {
				// set all bits in dimension dim with
				// bit position < max_bp to the value of corresponding bits of the
				// lower_bound
				for (size_t bit_pos = dim; bit_pos < index_dim * KEY_SIZE_IN_BITS - 1;
					 	bit_pos += index_dim) {
					if (bit_pos >= max_bp) {
						break;
					}
					bit_array_assign_bit(result, bit_pos,
										 bit_array_get_bit(lower_bound, bit_pos));
				}
			}
		} else {
			// nip has fallen below the minimum in dim
			// set all bits in dimension dim to the value of
			// corresponding bits of the lower_bound because the minimum would not
			// be exceeded otherwise
			for (size_t bit_pos = dim; bit_pos < index_dim * KEY_SIZE_IN_BITS - 1;
				 	bit_pos += index_dim) {
				bit_array_assign_bit(result, bit_pos,
									 bit_array_get_bit(lower_bound, bit_pos));
			}
		}
	}

	bit_array_set_bit(result, max_bp);
	return result;
}

static BIT_ARRAY*
interleave_keys(BIT_ARRAY** const keys, size_t size) {
    BIT_ARRAY* result = bit_array_create(size * KEY_SIZE_IN_BITS);
    for (size_t i = 0; i < size; i++) {
        for (size_t j = 0; j < KEY_SIZE_IN_BITS; j++) {
            if (bit_array_get_bit(keys[i], j) == 1) {
                bit_array_set_bit(result, size * j + i);
            }
        }
    }
    return result;
}

static BIT_ARRAY*
mp_decode_part(const char *mp, uint32_t part_count,
               const struct index_def *index_def, uint32_t even) {
    BIT_ARRAY* key_parts[part_count / 2];
    for (uint32_t j = 0; j < part_count; ++j) {
        uint32_t i = j / 2;
        if (j % 2 != even) {
            mp_next(&mp);
            continue;
        }
        if (mp_typeof(*mp) == MP_NIL) {
            if (j % 2 == 0) {
                key_parts[i] = zeros(1);
            } else {
                key_parts[i] = ones(1);
            }
        } else {
            switch (index_def->key_def->parts->type) {
                case FIELD_TYPE_UNSIGNED: {
                    uint64_t value = mp_decode_uint(&mp);
                    key_parts[i] = bit_array_create_word64(value);
                    break;
                }
                case FIELD_TYPE_INTEGER: {
                    int64_t value = mp_decode_int(&mp);
                    uint64_t* u_value = (void*)(&value);
                    key_parts[i] = bit_array_create_word64(*u_value);
                    bit_array_toggle_bit(key_parts[i], 63);
                    break;
                }
                case FIELD_TYPE_NUMBER: {
                    double value = mp_decode_double(&mp);
                    uint64_t* u_value = (void*)(&value);
                    key_parts[i] = bit_array_create_word64(*u_value);
                    bit_array_toggle_bit(key_parts[i], 63);
                    break;
                }
                case FIELD_TYPE_STRING: {
                    uint32_t value_len = sizeof(uint64_t);
                    const char* value = mp_decode_str(&mp, &value_len);
                    // TODO: copy first four bytes
                    uint64_t* u_value = (void*)(&value);
                    key_parts[i] = bit_array_create_word64(*u_value);
                    break;
                }
                default:
                    assert(false);
            }
        }
    }
    BIT_ARRAY* result = interleave_keys(key_parts, part_count / 2);
    for (uint32_t i = 0; i < part_count / 2; ++i) {
        bit_array_free(key_parts[i]);
    }
    bit_array_print(result, stdout);
    return result;
}

static BIT_ARRAY*
mp_decode_key(const char *mp, uint32_t part_count, const struct index_def *index_def) {
    BIT_ARRAY* key_parts[part_count];
    for (uint32_t i = 0; i < part_count; ++i) {
        switch (index_def->key_def->parts->type) {
            case FIELD_TYPE_UNSIGNED: {
                uint64_t value = mp_decode_uint(&mp);
                key_parts[i] = bit_array_create_word64(value);
                break;
            }
            case FIELD_TYPE_INTEGER: {
                int64_t value = mp_decode_int(&mp);
                uint64_t* u_value = (void*)(&value);
                key_parts[i] = bit_array_create_word64(*u_value);
                bit_array_toggle_bit(key_parts[i], 63);
                break;
            }
            case FIELD_TYPE_NUMBER: {
                double value = mp_decode_double(&mp);
                uint64_t* u_value = (void*)(&value);
                key_parts[i] = bit_array_create_word64(*u_value);
                bit_array_toggle_bit(key_parts[i], 63);
                break;
            }
            case FIELD_TYPE_STRING: {
                uint32_t value_len = sizeof(uint64_t);
                const char* value = mp_decode_str(&mp, &value_len);
                // TODO: copy first four bytes
                uint64_t* u_value = (void*)(&value);
                key_parts[i] = bit_array_create_word64(*u_value);
                break;
            }
            default:
                assert(false);
        }
        bit_array_print(key_parts[i], stdout);
    }
    BIT_ARRAY* result = interleave_keys(key_parts, part_count);
    for (uint32_t i = 0; i < part_count; ++i) {
        bit_array_free(key_parts[i]);
    }
    bit_array_print(result, stdout);
    return result;
}

// Extract z-address from tuple
static BIT_ARRAY*
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
	struct memtx_zcurve_key_data key_data;
	struct memtx_zcurve_data current;

	BIT_ARRAY* lower_bound;
	BIT_ARRAY* upper_bound;

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
		bit_array_free(it->lower_bound);
	}
	if (it->upper_bound != NULL) {
		bit_array_free(it->upper_bound);
	}
	if (tuple != NULL)
		tuple_unref(tuple);
	mempool_free(it->pool, it);
}

static int
tree_iterator_dummie(struct iterator *iterator, struct tuple **ret)
{
    printf("tree_iterator_dummie\n");
	(void)iterator;
	*ret = NULL;
	return 0;
}

static int
tree_iterator_next(struct iterator *iterator, struct tuple **ret)
{
    printf("tree_iterator_next\n");
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
	struct memtx_zcurve_data *res =
		memtx_zcurve_iterator_get_elem(it->tree, &it->tree_iterator);

	if (res == NULL || (it->lower_bound != NULL &&
		it->upper_bound != NULL &&
		bit_array_cmp(res->z_address, it->upper_bound) > 0)) {

		iterator->next = tree_iterator_dummie;
		it->current.tuple = NULL;
		// TODO: free memory?
		it->current.z_address = NULL;
		*ret = NULL;
	} else {
		*ret = res->tuple;
		tuple_ref(*ret);
		it->current = *res;
		if (it->upper_bound != NULL) {
			if (is_relevant(it->lower_bound, it->upper_bound,
					it->current.z_address)) {
				printf("is_relevant %s\n", tuple_str(it->current.tuple));
			} else {
				printf("is not relevant %s\n", tuple_str(it->current.tuple));
				BIT_ARRAY* next_zvalue = get_next_zvalue(it->current.z_address,
						it->lower_bound, it->upper_bound);
				struct memtx_zcurve_key_data next_value = {
						.z_address = next_zvalue,
						.part_count = it->key_data.part_count,
				};
				it->tree_iterator = memtx_zcurve_lower_bound(it->tree, &next_value, NULL);
				res = memtx_zcurve_iterator_get_elem(it->tree, &it->tree_iterator);
				if (res != NULL) {
					*ret = res->tuple;
					tuple_ref(*ret);
					it->current = *res;
				}
				bit_array_print(next_zvalue, stdout);
			}
		}
	}
	return 0;
}

static int
tree_iterator_prev(struct iterator *iterator, struct tuple **ret)
{
    printf("tree_iterator_prev\n");
	struct tree_iterator *it = tree_iterator(iterator);
	assert(it->current.tuple != NULL && it->current.z_address != NULL);
	struct memtx_zcurve_data *check =
		memtx_zcurve_iterator_get_elem(it->tree, &it->tree_iterator);
	if (check == NULL || !memtx_zcurve_data_identical(check, &it->current)) {
		it->tree_iterator =
			memtx_zcurve_lower_bound_elem(it->tree, it->current, NULL);
	}
	memtx_zcurve_iterator_prev(it->tree, &it->tree_iterator);
	tuple_unref(it->current.tuple);
	struct memtx_zcurve_data *res =
		memtx_zcurve_iterator_get_elem(it->tree, &it->tree_iterator);
	if (!res) {
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

static int
tree_iterator_next_equal(struct iterator *iterator, struct tuple **ret)
{
    printf("tree_iterator_next_equal\n");
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
	if (it->key_data.z_address_end == NULL) {
	    if (res == NULL ||
	    		memtx_zcurve_compare_key(res,
						  &it->key_data,
						  it->index_def->key_def) != 0) {
			iterator->next = tree_iterator_dummie;
			it->current.tuple = NULL;
			it->current.z_address = NULL;
			*ret = NULL;
		} else {
			*ret = res->tuple;
			tuple_ref(*ret);
			it->current = *res;
		}
	} else {
//		if (res == NULL ||
//			bit_array_cmp(res->z_address, it->key_data.z_address_end) < 0) {
				iterator->next = tree_iterator_dummie;
				it->current.tuple = NULL;
				it->current.z_address = NULL;
				*ret = NULL;
//		} else {
//
//		}
	}
	return 0;
}

static int
tree_iterator_prev_equal(struct iterator *iterator, struct tuple **ret)
{
    printf("tree_iterator_prev_equal\n");
	struct tree_iterator *it = tree_iterator(iterator);
	assert(it->current.tuple != NULL && it->current.z_address != NULL);
	struct memtx_zcurve_data *check =
		memtx_zcurve_iterator_get_elem(it->tree, &it->tree_iterator);
	if (check == NULL || !memtx_zcurve_data_identical(check, &it->current)) {
		it->tree_iterator =
			memtx_zcurve_lower_bound_elem(it->tree, it->current, NULL);
	}
	memtx_zcurve_iterator_prev(it->tree, &it->tree_iterator);
	tuple_unref(it->current.tuple);
	struct memtx_zcurve_data *res =
		memtx_zcurve_iterator_get_elem(it->tree, &it->tree_iterator);
	/* Use user key def to save a few loops. */
	if (res == NULL ||
            memtx_zcurve_compare_key(res,
					  &it->key_data,
					  it->index_def->key_def) != 0) {
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
    printf("tree_iterator_set_next_method %d\n", it->type);
	assert(it->current.tuple != NULL && it->current.z_address != NULL);
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
    printf("tree_iterator_start\n");
	*ret = NULL;
	struct tree_iterator *it = tree_iterator(iterator);
	it->base.next = tree_iterator_dummie;
	const struct memtx_zcurve *tree = it->tree;
	enum iterator_type type = it->type;
	bool exact = false;
	assert(it->current.tuple == NULL);
	assert(it->current.z_address == NULL);
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
			// TODO: add search of first relevant value
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
			 * Because of limitations of tree search API we use use
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

	struct memtx_zcurve_data *res =
		memtx_zcurve_iterator_get_elem(it->tree, &it->tree_iterator);
	if (!res)
		return 0;
	*ret = res->tuple;
	tuple_ref(*ret);
	it->current = *res;
	tree_iterator_set_next_method(it);
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
	return memtx_zcurve_mem_used(&index->tree);
}

static int
memtx_zcurve_index_random(struct index *base, uint32_t rnd, struct tuple **result)
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
	struct memtx_zcurve_key_data key_data;
	key_data.key = key;
	key_data.z_address = mp_decode_key(key, part_count, index->base.def);
	key_data.part_count = part_count;
	struct memtx_zcurve_data *res = memtx_zcurve_find(&index->tree, &key_data);
	*result = res != NULL ? res->tuple : NULL;
	bit_array_free(key_data.z_address);
	return 0;
}

static int
memtx_zcurve_index_replace(struct index *base, struct tuple *old_tuple,
			 struct tuple *new_tuple, enum dup_replace_mode mode,
			 struct tuple **result)
{
	struct memtx_zcurve_index *index = (struct memtx_zcurve_index *)base;
	//struct key_def *cmp_def = memtx_zcurve_cmp_def(&index->tree);
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
    // FIXME: Check that it works
	it->key_data.key = key;
	it->key_data.z_address = NULL;
	it->key_data.z_address_end = NULL;
	it->lower_bound = NULL;
	it->upper_bound = NULL;
	printf("%d %d\n", base->def->key_def->part_count, part_count);
	if (base->def->key_def->part_count == part_count) {
		// TODO: remove key_data
        it->key_data.z_address = mp_decode_key(key, part_count, index->base.def);
		it->lower_bound = mp_decode_key(key, part_count, index->base.def);
		it->upper_bound = ones(part_count);
	} else if (base->def->key_def->part_count * 2 == part_count) {
		it->lower_bound = mp_decode_part(key, part_count, index->base.def, 0);
		it->upper_bound = mp_decode_part(key, part_count, index->base.def, 1);
		it->key_data.z_address = mp_decode_part(key, part_count, index->base.def, 0);
		// TODO: remove key_data
		it->key_data.z_address_end = mp_decode_part(key, part_count, index->base.def, 1);
	} else {
		// error?
	}

	it->key_data.part_count = part_count;
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
//	struct key_def *cmp_def = memtx_zcurve_cmp_def(&index->tree);
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
	struct key_def *cmp_def = memtx_zcurve_cmp_def(&index->tree);
	qsort_arg(index->build_array, index->build_array_size,
		  sizeof(index->build_array[0]), memtx_zcurve_qcompare, cmp_def);
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
	struct key_def *cmp_def;
	cmp_def = def->opts.is_unique && !def->key_def->is_nullable ?
			index->base.def->key_def : index->base.def->cmp_def;

	memtx_zcurve_create(&index->tree, cmp_def, memtx_index_extent_alloc,
			  memtx_index_extent_free, memtx);
	return &index->base;
}
