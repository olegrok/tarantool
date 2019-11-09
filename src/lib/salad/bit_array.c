#include "bit_array.h"

#include <stdlib.h>
#include <assert.h>
#include <string.h>

#define WORD_MAX  (~0ULL)
#define WORD_SIZE 64ULL

inline size_t
bit_array_bsize(word_addr_t num_of_words)
{
	assert(num_of_words > 0);
	return sizeof(bit_array) + num_of_words * sizeof(word_t);
}

bit_array*
bit_array_create(word_addr_t num_of_words)
{
	size_t size = bit_array_bsize(num_of_words);

	bit_array *bitarr = (bit_array*)calloc(1, size);
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
bit_array_add(bit_array* src, const bit_array* add)
{
	word_addr_t max_words = src->num_of_words;

	char carry = 0;

	word_addr_t i;
	word_t word1, word2;

	for(i = 0; i < max_words; i++) {
		word1 = src->words[i];
		word2 = add->words[i];

		src->words[i] = word1 + word2 + carry;
		carry = WORD_MAX - word1 < word2 || WORD_MAX - word1 - word2 < (word_t)carry;
	}
}

void
bit_array_add_uint64(bit_array* bitarr, uint64_t value)
{
	if(value == 0) {
		return;
	}

	word_addr_t i;

	for(i = 0; i < bitarr->num_of_words; i++) {
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

int
bit_array_cmp(const bit_array* left, const bit_array* right)
{
	assert(left->num_of_words == right->num_of_words);

	word_t word1, word2;
	word_addr_t num_of_words = left->num_of_words;

	for(word_addr_t i = num_of_words - 1;; i--)
	{
		word1 = left->words[i];
		word2 = right->words[i];
		if (word1 != word2)
			return (word1 > word2 ? 1 : -1);
		if (i == 0)
			break;
	}

	return 0;
}

void
bit_array_set_all(bit_array* bitarr)
{
	memset(bitarr->words, 0xFF, bitarr->num_of_words * sizeof(word_t));
}

void
bit_array_clear_all(bit_array* bitarr)
{
	memset(bitarr->words, 0, bitarr->num_of_words * sizeof(word_t));
}


bit_index_t
bit_array_length(const bit_array* bit_arr)
{
	return bit_arr->num_of_words * WORD_SIZE;
}

bit_array*
bit_array_clone(const bit_array* bitarr)
{
	bit_array* cpy = bit_array_create(bitarr->num_of_words);

	if (cpy == NULL) {
		return NULL;
	}

	memcpy(cpy->words, bitarr->words, bitarr->num_of_words * sizeof(word_t));

	return cpy;
}

void
bit_array_copy(const bit_array* src, bit_array* dst)
{
	assert(src->num_of_words == dst->num_of_words);
	memcpy(dst->words, src->words, src->num_of_words * sizeof(word_t));
}

void
bit_array_shift_left(bit_array* bitarr, bit_index_t shift_dist)
{
	if (shift_dist == 0) {
		return;
	} else if (shift_dist >= bitarr->num_of_words * WORD_SIZE) {
		bit_array_clear_all(bitarr);
		return;
	}

	unsigned int limit = bitarr->num_of_words;
	unsigned int offset = shift_dist / WORD_SIZE;
	unsigned int remainder = shift_dist % WORD_SIZE;
	for (int i = (int)(limit - offset - 1); i >= 0; --i) {
		unsigned long upper, lower;
		if (remainder && i > 0)
			lower = bitarr->words[i - 1] >> (WORD_SIZE - remainder);
		else
			lower = 0;
		upper = bitarr->words[i] << remainder;
		bitarr->words[i + offset] = lower | upper;
	}
	if (offset)
		memset(bitarr->words, 0, offset * sizeof(uint64_t));
}

static inline void
bit_array_or_internal(word_t * restrict dst, const word_t * restrict src, size_t num)
{
#pragma omp simd
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
bit_array_and_internal(word_t * restrict dst, const word_t * restrict src, size_t num)
{
#pragma omp simd
	for(size_t i = 0; i < num; i++)
		dst[i] &= src[i];
}

void
bit_array_and(bit_array* dst, const bit_array* src)
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

#include <stdlib.h>
#include "bit_array.h"

#define LOOKUP_TABLE_SIZE 256
#define LOOKUP_TABLE_BSIZE (256 * sizeof(bit_array*))

static void
fill_table(bit_array **table, size_t dim, uint8_t shift)
{
	const size_t BIT_COUNT = 8;
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

static int
bit_array_interleave_new_lookup_table(size_t dim, bit_array** table,
									  size_t shift)
{
	for (size_t i = 0; i < LOOKUP_TABLE_SIZE; i++) {
		table[i] = bit_array_create(dim);
		if (table[i] == NULL) {
			free(table);
			return -1;
		}
	}

	fill_table(table, dim, shift);
	return 0;
}

static void
bit_array_interleave_free_lookup_table(bit_array **table)
{
	for (size_t i = 0; i < LOOKUP_TABLE_SIZE; i++) {
		bit_array_free(table[i]);
	}
	free(table);
}

bit_array***
bit_array_interleave_new_lookup_tables(size_t dim)
{
	bit_array ***tables = malloc(dim * sizeof(bit_array**));
	if (tables == NULL) {
		return NULL;
	}

	for(size_t i = 0; i < dim; i++) {
		tables[i] = malloc(LOOKUP_TABLE_BSIZE);
		if (tables[i] == 0) {
			for(size_t j = 0; j < i; j++) {
				free(tables[j]);
			}
			free(tables);
			return NULL;
		}
	}

	for(size_t i = 0; i < dim; i++) {
		int res = bit_array_interleave_new_lookup_table(dim, tables[i], i);
		if (res < 0) {
			for(size_t j = 0; j < i; j++) {
				bit_array_interleave_free_lookup_table(tables[j]);
			}
			free(tables);
			return NULL;
		}
	}

	return tables;
}

void
bit_array_interleave_free_lookup_tables(bit_array ***tables, size_t dim)
{
	for (size_t i = 0; i < dim; i++) {
		bit_array_interleave_free_lookup_table(tables[i]);
	}
	free(tables);
}

int
bit_array_interleave(bit_array ***tables, size_t dim,
					 const uint64_t *in, bit_array *out)
{
	const size_t octets_count = 8;
	const size_t octet_size = 8;

	bit_array *tmp = bit_array_create(dim);
	if (tmp == NULL)
		return -1;

	for (size_t i = 0; i < octets_count; i++) {
		size_t shift = octet_size * i;
		for (size_t j = 0; j < dim; j++) {
			uint8_t octet = (in[j] >> shift);
			const bit_array *value = tables[j][octet];
			bit_array_or(tmp, value);
		}
		bit_array_shift_left(tmp, dim * shift);
		bit_array_or(out, tmp);
		bit_array_clear_all(tmp);
	}
	bit_array_free(tmp);
	return 0;
}
