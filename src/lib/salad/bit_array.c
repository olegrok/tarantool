#include "bit_array.h"

#include <stdlib.h>
#include <assert.h>
#include <string.h>

#define WORD_MAX  (~0ULL)
#define WORD_SIZE 64ULL
#define LOOKUP_TABLE_SIZE 256
#define LOOKUP_TABLE_BSIZE (LOOKUP_TABLE_SIZE * sizeof(bit_array*))

size_t
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

void
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
			// Carry is absorbed
			bitarr->words[i] += value;
			break;
		}
	}
}

int
bit_array_cmp(const bit_array* left, const bit_array* right)
{
	assert(left->num_of_words == right->num_of_words);
	word_addr_t i;
	word_t word1, word2;
	word_addr_t num_of_words = left->num_of_words;

	for(i = num_of_words - 1;; i--)
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

	if(cpy == NULL) {
		return NULL;
	}

	memcpy(cpy->words, bitarr->words, bitarr->num_of_words * sizeof(word_t));

	return cpy;
}

static inline word_t
get_word(const bit_array* bitarr, bit_index_t start)
{
	word_addr_t word_index = bitset64_wrd(start);
	word_offset_t word_offset = bitset64_idx(start);

	word_t result = bitarr->words[word_index] >> word_offset;

	word_offset_t bits_taken = WORD_SIZE - word_offset;

	if(word_offset > 0 && start + bits_taken < bitarr->num_of_words * WORD_SIZE) {
		result |= bitarr->words[word_index + 1] << (WORD_SIZE - word_offset);
	}

	return result;
}

static inline void
set_word(bit_array* bitarr, bit_index_t start, word_t word)
{
	word_addr_t word_index = bitset64_wrd(start);
	word_offset_t word_offset = bitset64_idx(start);

	if(word_offset == 0) {
		bitarr->words[word_index] = word;
	} else {
		bitarr->words[word_index]
				= (word << word_offset) |
				  (bitarr->words[word_index] & bitmask64(word_offset));

		if(word_index+1 < bitarr->num_of_words) {
			bitarr->words[word_index + 1]
					= (word >> (WORD_SIZE - word_offset)) |
					  (bitarr->words[word_index + 1] & (WORD_MAX << word_offset));
		}
	}
}

static void
array_copy(bit_array* dst, bit_index_t dstindx,
		   const bit_array* src, bit_index_t srcindx,
		   bit_index_t length)
{
	word_addr_t num_of_full_words = length / WORD_SIZE;
	word_addr_t i;

	word_offset_t bits_in_last_word = WORD_SIZE;

	if(dst == src && srcindx > dstindx) {
		for(i = 0; i < num_of_full_words; i++) {
			word_t word = get_word(src, srcindx + i * WORD_SIZE);
			set_word(dst, dstindx + i * WORD_SIZE, word);
		}

		word_t src_word = get_word(src, srcindx + i * WORD_SIZE);
		word_t dst_word = get_word(dst, dstindx + i * WORD_SIZE);

		word_t mask = bitmask64(bits_in_last_word);
		word_t word = bitmask_merge(src_word, dst_word, mask);

		set_word(dst, dstindx + num_of_full_words * WORD_SIZE, word);
	} else {
		for(i = 0; i < num_of_full_words; i++) {
			word_t word = get_word(src, srcindx + length - (i + 1) * WORD_SIZE);
			set_word(dst, dstindx + length - (i + 1) * WORD_SIZE, word);
		}

		word_t src_word = get_word(src, srcindx);
		word_t dst_word = get_word(dst, dstindx);

		word_t mask = bitmask64(bits_in_last_word);
		word_t word = bitmask_merge(src_word, dst_word, mask);
		set_word(dst, dstindx, word);
	}
}

static inline void
set_region(bit_array* bitarr, bit_index_t start, bit_index_t length)
{
	if(length == 0) return;

	word_addr_t first_word = bitset64_wrd(start);
	word_addr_t last_word = bitset64_wrd(start + length - 1);
	word_offset_t foffset = bitset64_idx(start);
	word_offset_t loffset = bitset64_idx(start + length - 1);

	if(first_word == last_word) {
		word_t mask = bitmask64(length) << foffset;
		bitarr->words[first_word] &= ~mask;
	} else {
		bitarr->words[first_word] &=  bitmask64(foffset);
		word_addr_t i;

		for(i = first_word + 1; i < last_word; i++)
			bitarr->words[i] = (word_t)0;
		bitarr->words[last_word] &= ~bitmask64(loffset+1);
	}
}

void
bit_array_shift_left(bit_array* bitarr, bit_index_t shift_dist)
{
	if(shift_dist >= bitarr->num_of_words * WORD_SIZE) {
		bit_array_clear_all(bitarr);
		return;
	} else if(shift_dist == 0) {
		return;
	}

	bit_index_t cpy_length = bitarr->num_of_words * WORD_SIZE - shift_dist;
	array_copy(bitarr, shift_dist, bitarr, 0, cpy_length);
	set_region(bitarr, 0, shift_dist);
}

void
bit_array_or(bit_array* dst, const bit_array* src1, const bit_array* src2)
{
	assert(dst->num_of_words == src1->num_of_words);
	assert(src1->num_of_words == src2->num_of_words);
	for(size_t i = 0; i < dst->num_of_words; i++)
		dst->words[i] = src1->words[i] | src2->words[i];
}

static void
fill_table(bit_array **table, size_t dim, uint8_t shift)
{
	uint8_t one = 1;
	const size_t BIT_COUNT = 8;
	for(size_t i = 0; i < LOOKUP_TABLE_SIZE; i++) {
		uint8_t num = i;
		for (size_t j = 0; j < BIT_COUNT; j++) {
			if (num & one) {
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
			bit_array_or(tmp, tmp, value);
		}
		bit_array_shift_left(tmp, dim * shift);
		bit_array_or(out, out, tmp);
		bit_array_clear_all(tmp);
	}
	bit_array_free(tmp);
	return 0;
}
