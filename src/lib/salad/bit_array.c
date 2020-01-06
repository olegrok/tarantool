#include "bit_array.h"

#include <stdlib.h>
#include <assert.h>
#include <string.h>

#define WORD_MAX  (~0ULL)
#define WORD_SIZE 64ULL
#define LOOKUP_TABLE_SIZE 256
#define BIT_COUNT 8

inline size_t
bit_array_bsize(word_addr_t num_of_words)
{
	assert(num_of_words > 0);
	return sizeof(bit_array) + num_of_words * sizeof(word_t);
}

bit_array *
bit_array_create(word_addr_t num_of_words)
{
	size_t size = bit_array_bsize(num_of_words);

	bit_array *bitarr = (bit_array *)calloc(1, size);
	if (bitarr == NULL) {
		return NULL;
	}

	bitarr->words = (void*)(bitarr + 1);
	bitarr->num_of_words = num_of_words;
	return bitarr;
}

inline void
bit_array_free(bit_array *array)
{
	free(array);
}

void
bit_array_add(bit_array *src, const bit_array *add)
{
	assert(src->num_of_words == add->num_of_words);
	word_addr_t num_of_words = src->num_of_words;

	char carry = 0;
	word_t word1, word2;

	for(size_t i = 0; i < num_of_words; i++) {
		word1 = src->words[i];
		word2 = add->words[i];

		src->words[i] = word1 + word2 + carry;
		carry = WORD_MAX - word1 < word2 || WORD_MAX - word1 - word2 < (word_t)carry;
	}
}

void
bit_array_add_uint64(bit_array *bitarr, uint64_t value)
{
	if(value == 0) {
		return;
	}

	for(word_addr_t i = 0; i < bitarr->num_of_words; i++) {
		if(WORD_MAX - bitarr->words[i] < value) {
			bitarr->words[i] += value;
			value = 1;
		} else {
			/* Carry is absorbed */
			bitarr->words[i] += value;
			break;
		}
	}
}

static inline int
bit_array_cmp_internal(const word_t *restrict left, const word_t *restrict right,
					   size_t num)
{
	for(word_addr_t i = num - 1;; i--)
	{
		if (left[i] != right[i])
			return (left[i] > right[i] ? 1 : -1);
		if (i == 0)
			break;
	}
	return 0;
}

int
bit_array_cmp(const bit_array *left, const bit_array *right)
{
	assert(left->num_of_words == right->num_of_words);

	word_addr_t num_of_words = left->num_of_words;
	return bit_array_cmp_internal(left->words, right->words, num_of_words);
}

void
bit_array_set_all(bit_array *bitarr)
{
	memset(bitarr->words, 0xFF, bitarr->num_of_words * sizeof(word_t));
}

void
bit_array_clear_all(bit_array *bitarr)
{
	memset(bitarr->words, 0, bitarr->num_of_words * sizeof(word_t));
}


bit_index_t
bit_array_length(const bit_array *bit_arr)
{
	return bit_arr->num_of_words * WORD_SIZE;
}

bit_array *
bit_array_copy(bit_array *restrict dst, const bit_array *restrict src)
{
	assert(dst->num_of_words == src->num_of_words);
	memcpy(dst->words, src->words, src->num_of_words * sizeof(word_t));

	return dst;
}

bit_array *
bit_array_clone(const bit_array *src)
{
	bit_array *dst = bit_array_create(src->num_of_words);

	if (dst == NULL) {
		return NULL;
	}

	return bit_array_copy(dst, src);
}

void
bit_array_shift_left(bit_array *bitarr, bit_index_t shift_dist)
{
	if (shift_dist == 0) {
		return;
	} else if (shift_dist >= bitarr->num_of_words * WORD_SIZE) {
		bit_array_clear_all(bitarr);
		return;
	}

	const size_t limit = bitarr->num_of_words;
	const size_t offset = shift_dist / WORD_SIZE;
	const size_t remainder = shift_dist % WORD_SIZE;

	word_t upper = 0, lower = 0;
	for (size_t i = limit - offset - 1;; --i) {
		if (remainder && i > 0)
			lower = bitarr->words[i - 1] >> (WORD_SIZE - remainder);
		else
			lower = 0;
		upper = bitarr->words[i] << remainder;
		bitarr->words[i + offset] = lower | upper;

		if (i == 0)
			break;
	}

	if (offset)
		memset(bitarr->words, 0, offset * sizeof(uint64_t));
}

static inline void
bit_array_or_internal(word_t *restrict dst, const word_t *restrict src, size_t num)
{
	for(size_t i = 0; i < num; i++)
		dst[i] |= src[i];
}

void
bit_array_or(bit_array *dst, const bit_array *src)
{
	assert(dst->num_of_words == src->num_of_words);
	size_t num_of_words = dst->num_of_words;
	bit_array_or_internal(dst->words, src->words, num_of_words);
}

static inline void
bit_array_and_internal(word_t *restrict dst, const word_t *restrict src, size_t num)
{
	for(size_t i = 0; i < num; i++)
		dst[i] &= src[i];
}

void
bit_array_and(bit_array *restrict dst, const bit_array *restrict src)
{
	assert(dst->num_of_words == src->num_of_words);
	size_t num_of_words = dst->num_of_words;
	bit_array_and_internal(dst->words, src->words, num_of_words);
}

uint64_t
bit_array_get_word(const bit_array *bitarr, bit_index_t num)
{
	assert(num < bitarr->num_of_words);
	return bitarr->words[num];
}

static void
fill_table(bit_array **table, size_t dim, uint8_t shift)
{
	for(size_t i = 0; i < LOOKUP_TABLE_SIZE; i++) {
		uint8_t num = i;
		for (size_t j = 0; j < BIT_COUNT; j++) {
			if (num & 1U) {
				bit_array_set(table[i], j * dim + shift);
			}
			num >>= 1ULL;
		}
	}
}

static void
bit_array_interleave_free_dim_lookup_table(bit_array **table)
{
	if (table == NULL) {
		return;
	}

	for (size_t i = 0; i < LOOKUP_TABLE_SIZE; i++) {
		if (table[i] != NULL) {
			bit_array_free(table[i]);
			table[i] = NULL;
		}
	}
	free(table);
}

struct bit_array_interleave_lookup_table *
bit_array_interleave_new_lookup_tables(size_t dim)
{
	assert(dim > 0);
	/* Allocate memory for structure */
	struct bit_array_interleave_lookup_table *table = calloc(1,
															 sizeof(struct bit_array_interleave_lookup_table));

	if (table == NULL) {
		return NULL;
	}
	table->dim = dim;

	/* Allocate memory for buffer */
	table->buffer = bit_array_create(dim);
	if (table->buffer == NULL) {
		free(table);
		return NULL;
	}

	/* Allocate memory for lookup tables for each dimension */
	table->tables = calloc(dim, sizeof(bit_array***));
	if (table->tables == NULL) {
		free(table->buffer);
		free(table);
		return NULL;
	}

	for(size_t i = 0; i < dim; i++) {
		table->tables[i] = calloc(LOOKUP_TABLE_SIZE, sizeof(bit_array**));
		if (table->tables[i] == NULL) {
			bit_array_interleave_free_lookup_tables(table);
			return NULL;
		}

		for (size_t j = 0; j < LOOKUP_TABLE_SIZE; j++) {
			table->tables[i][j] = bit_array_create(dim);
			if (table->tables[i] == NULL) {
				bit_array_interleave_free_lookup_tables(table);
				return NULL;
			}
		}

		fill_table(table->tables[i], dim, i);
	}

	return table;
}

void
bit_array_interleave_free_lookup_tables(
		struct bit_array_interleave_lookup_table *table)
{

	if (table->tables != NULL) {
		for (size_t i = 0; i < table->dim; i++) {
			bit_array_interleave_free_dim_lookup_table(table->tables[i]);
			table->tables[i] = NULL;
		}

		free(table->tables);
		table->tables = NULL;
	}

	if (table->buffer != NULL) {
		free(table->buffer);
		table->buffer = NULL;
	}

	table->dim = 0;
	free(table);
}

int
bit_array_interleave(struct bit_array_interleave_lookup_table *table,
					 const uint64_t *in, bit_array *out)
{
	const size_t octets_count = 8;
	const size_t octet_size = 8;

	bit_array_clear_all(table->buffer);
	for (size_t i = 0; i < octets_count; i++) {
		size_t shift = octet_size * i;
		for (size_t j = 0; j < table->dim; j++) {
			uint8_t octet = (in[j] >> shift);
			const bit_array *value = table->tables[j][octet];
			bit_array_or(table->buffer, value);
		}
		bit_array_shift_left(table->buffer, table->dim * shift);
		bit_array_or(out, table->buffer);
		bit_array_clear_all(table->buffer);
	}

	return 0;
}
