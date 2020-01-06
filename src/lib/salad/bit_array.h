#ifndef BIT_ARRAY_H
#define BIT_ARRAY_H

#include <inttypes.h>
#include <stdlib.h>

typedef struct bit_array bit_array;
typedef uint64_t word_t, word_addr_t, bit_index_t;

#ifdef __cplusplus
extern "C" {
#endif

#include "bit_array_macros.h"

struct bit_array
{
	word_addr_t num_of_words;
	word_t *words;
};

size_t
bit_array_bsize(word_addr_t num_of_words);

bit_array*
bit_array_create(word_addr_t num_of_words);

void
bit_array_free(bit_array *);

void
bit_array_add(bit_array *src, const bit_array *add);

void
bit_array_add_uint64(bit_array* bitarr, uint64_t value);

int
bit_array_cmp(const bit_array *left, const bit_array *right);

void
bit_array_set_all(bit_array* bitarr);

void
bit_array_clear_all(bit_array* bitarr);

void
bit_array_shift_left(bit_array* bitarr, bit_index_t shift_dist);

bit_index_t
bit_array_length(const bit_array* bit_arr);

bit_array *
bit_array_clone(const bit_array* bitarr);

bit_array *
bit_array_copy(bit_array *dst, const bit_array *src);

#define bit_array_get(arr,i)      bitset_get((arr)->words, i)
#define bit_array_set(arr,i)      bitset_set((arr)->words, i)
#define bit_array_clear(arr,i)    bitset_del((arr)->words, i)
#define bit_array_toggle(arr,i)   bitset_tgl((arr)->words, i)
#define bit_array_assign(arr,i,c) bitset_cpy((arr)->words,i,c)

struct bit_array_interleave_lookup_table {
	/**
	 * Contains octet lookup table with
	 * shifts for each dimension
	 */
	bit_array ***tables;
	/**
	 * Preallocated buffer using in process of interleaving
	 */
	bit_array *buffer;
	/**
	 * Amount of dimensions
	 */
	size_t dim;
};

struct bit_array_interleave_lookup_table *
bit_array_interleave_new_lookup_tables(size_t dim);

void
bit_array_interleave_free_lookup_tables(
		struct bit_array_interleave_lookup_table *table);

int
bit_array_interleave(struct bit_array_interleave_lookup_table *table,
					 const uint64_t *in, bit_array *out);

#ifdef __cplusplus
}
#endif

#endif //BIT_ARRAY_H
