#ifndef TARANTOOL_BOX_MEMTX_ZCURVE_H_INCLUDED
#define TARANTOOL_BOX_MEMTX_ZCURVE_H_INCLUDED
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
#include <stddef.h>
#include <stdint.h>

#include "index.h"
#include "memtx_engine.h"
#include <salad/bit_array.h>

#if defined(__cplusplus)
extern "C" {
#endif /* defined(__cplusplus) */

struct memtx_engine;

/**
 * Struct that is used as a key in BPS tree definition.
 */
struct memtx_zcurve_key_data
{
    /** Sequence of msgpacked search fields */
    BIT_ARRAY *key;
    /** Number of msgpacked search fields */
    uint32_t part_count;
};

/**
 * Struct that is used to store z_address and tuple.
 */
struct memtx_zcurve_element
{
    /** Z-address. Read here: https://en.wikipedia.org/wiki/Z-order_curve */
    BIT_ARRAY *z_address;
    struct tuple *tuple;
};

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
 memtx_zcurve_compare_key(const struct memtx_zcurve_element *element,
                       const struct memtx_zcurve_key_data *key_data,
                       struct key_def *def)
{
    (void)def;
    (void)element;
    (void)key_data;
    printf("element %p\n", element);
    printf("element->z_address %p\n", element->z_address);
    fflush(stdout);
    bit_array_print(element->z_address, stdout);
    printf("elem_b ");
    bit_array_print(key_data->key, stdout);
    return bit_array_cmp(element->z_address, key_data->key);
}

static inline int
memtx_zcurve_elem_compare(const struct memtx_zcurve_element *elem_a,
                        const struct memtx_zcurve_element *elem_b,
                        struct key_def *key_def)
{
    (void)key_def;
    printf("elem_a ");
    fflush(stdout);
    bit_array_print(elem_a->z_address, stdout);
    printf("elem_b ");
    bit_array_print(elem_b->z_address, stdout);
    return bit_array_cmp(elem_a->z_address, elem_b->z_address);
}

#define BPS_TREE_NAME memtx_zcurve
#define BPS_TREE_BLOCK_SIZE (512)
#define BPS_TREE_EXTENT_SIZE MEMTX_EXTENT_SIZE
#define BPS_TREE_COMPARE(a, b, arg) memtx_zcurve_elem_compare(a, b, arg)
#define BPS_TREE_COMPARE_KEY(a, b, arg) memtx_zcurve_compare_key(a, b, arg)
#define bps_tree_elem_t struct memtx_zcurve_element *
#define bps_tree_key_t struct memtx_zcurve_key_data *
#define bps_tree_arg_t struct key_def *

#include "salad/bps_tree.h"

#undef BPS_TREE_NAME
#undef BPS_TREE_BLOCK_SIZE
#undef BPS_TREE_EXTENT_SIZE
#undef BPS_TREE_COMPARE
#undef BPS_TREE_COMPARE_KEY
#undef bps_tree_elem_t
#undef bps_tree_key_t
#undef bps_tree_arg_t

struct memtx_zcurve_index {
    struct index base;
    struct memtx_zcurve tree;
    struct memtx_zcurve_element **build_array;
    size_t build_array_size, build_array_alloc_size;
    struct memtx_gc_task gc_task;
    struct memtx_zcurve_iterator gc_iterator;
};

struct index *
memtx_zcurve_index_new(struct memtx_engine *memtx, struct index_def *def);

#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */

#endif /* TARANTOOL_BOX_MEMTX_ZCURVE_H_INCLUDED */
