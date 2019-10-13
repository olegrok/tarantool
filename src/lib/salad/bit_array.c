#include "bit_array.h"

#include <stdlib.h>
#include <assert.h>
#include <string.h>

#define WORD_MAX  (~0ULL)
#define WORD_SIZE 64ULL

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
