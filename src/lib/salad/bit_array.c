#include "bit_array.h"

#include <assert.h>
#include <string.h>

#define WORD_MAX  (~0ULL)
#define WORD_SIZE 64ULL
#define LOOKUP_TABLE_SIZE 256
#define BIT_COUNT 8

inline size_t
bit_array_bsize(word_size_t num_of_words)
{
	assert(num_of_words > 0);
	return sizeof(bit_array) + num_of_words * sizeof(word_t);
}

bit_array *
bit_array_create(struct mempool *pool, word_size_t num_of_words)
{
	bit_array *bitarr = (bit_array *)mempool_alloc(pool);
	if (bitarr == NULL) {
		return NULL;
	}
	bit_array_clear_all(bitarr, num_of_words);
	return bitarr;
}

inline void
bit_array_free(struct mempool *pool, bit_array *array)
{
	mempool_free(pool, array);
}

void
bit_array_add(bit_array *src, const bit_array *add, word_size_t num_of_words)
{
	char carry = 0;
	word_t word1, word2;

	for(word_size_t i = 0; i < num_of_words; i++) {
		word1 = src[i];
		word2 = add[i];

		src[i] = word1 + word2 + carry;
		carry = WORD_MAX - word1 < word2 || WORD_MAX - word1 - word2 < (word_t)carry;
	}
}

void
bit_array_add_word(bit_array *bitarr, word_t value, word_size_t num_of_words)
{
	if(value == 0) {
		return;
	}

	for(word_size_t i = 0; i < num_of_words; i++) {
		if(WORD_MAX - bitarr[i] < value) {
			bitarr[i] += value;
			value = 1;
		} else {
			/* Carry is absorbed */
			bitarr[i] += value;
			break;
		}
	}
}

static inline int8_t
bit_array_cmp_internal(const word_t *restrict left, const word_t *restrict right,
					   word_size_t num)
{
	for(word_size_t i = num - 1;; i--)
	{
		if (left[i] != right[i])
			return (left[i] > right[i] ? 1 : -1);
		if (i == 0)
			break;
	}
	return 0;
}

int8_t
bit_array_cmp(const bit_array *left, const bit_array *right,
			  word_size_t num_of_words)
{
	return bit_array_cmp_internal(left, right, num_of_words);
}

void
bit_array_set_all(bit_array *bitarr, word_size_t num_of_words)
{
	memset(bitarr, 0xFF, num_of_words * sizeof(word_t));
}

void
bit_array_clear_all(bit_array *bitarr, word_size_t num_of_words)
{
	memset(bitarr, 0, num_of_words * sizeof(word_t));
}

bit_index_t
bit_array_length(word_size_t num_of_words)
{
	return num_of_words * WORD_SIZE;
}

bit_array *
bit_array_copy(bit_array *restrict dst, const bit_array *restrict src, word_size_t num_of_words)
{
	memcpy(dst, src, num_of_words * sizeof(word_t));
	return dst;
}

bit_array *
bit_array_clone(struct mempool *pool, const bit_array *src, word_size_t num_of_words)
{
	bit_array *dst = bit_array_create(pool, num_of_words);

	if (dst == NULL) {
		return NULL;
	}

	return bit_array_copy(dst, src, num_of_words);
}

void
bit_array_shift_left(bit_array *bitarr, bit_index_t shift_dist,
					 word_size_t num_of_words)
{
	if (shift_dist == 0) {
		return;
	} else if (shift_dist >= num_of_words * WORD_SIZE) {
		bit_array_clear_all(bitarr, num_of_words);
		return;
	}

	const uint8_t limit = num_of_words;
	const uint8_t offset = shift_dist / WORD_SIZE;
	const uint8_t remainder = shift_dist % WORD_SIZE;

	word_t upper = 0, lower = 0;
	for (uint8_t i = limit - offset - 1;; --i) {
		if (remainder && i > 0)
			lower = bitarr[i - 1] >> (WORD_SIZE - remainder);
		else
			lower = 0;
		upper = bitarr[i] << remainder;
		bitarr[i + offset] = lower | upper;

		if (i == 0)
			break;
	}

	if (offset)
		memset(bitarr, 0, offset * sizeof(word_t));
}

static inline void
bit_array_or_internal(word_t *restrict dst, const word_t *restrict src,
					  word_size_t num)
{
	for(word_size_t i = 0; i < num; i++)
		dst[i] |= src[i];
}

void
bit_array_or(bit_array *dst, const bit_array *src, word_size_t num_of_words)
{
	bit_array_or_internal(dst, src, num_of_words);
}

static inline void
bit_array_and_internal(word_t *restrict dst, const word_t *restrict src,
					   word_size_t num)
{
	for(word_size_t i = 0; i < num; i++)
		dst[i] &= src[i];
}

void
bit_array_and(bit_array *restrict dst, const bit_array *restrict src,
			  word_size_t num_of_words)
{
	bit_array_and_internal(dst, src, num_of_words);
}

word_t
bit_array_get_word(const bit_array *bitarr, word_size_t num)
{
	return bitarr[num];
}

static void
fill_table(bit_array **table, uint8_t dim, uint8_t shift)
{
	for (uint16_t i = 0; i < LOOKUP_TABLE_SIZE; i++) {
		uint8_t num = i;
		for (uint8_t j = 0; j < BIT_COUNT; j++) {
			if (num & 1U) {
				bit_array_set(table[i], j * dim + shift);
			}
			num >>= 1ULL;
		}
	}
}

static void
bit_array_interleave_free_dim_lookup_table(struct mempool *pool,
										   bit_array **table)
{
	if (table == NULL) {
		return;
	}

	for (uint16_t i = 0; i < LOOKUP_TABLE_SIZE; i++) {
		if (table[i] != NULL) {
			bit_array_free(pool, table[i]);
			table[i] = NULL;
		}
	}
	free(table);
}

struct bit_array_interleave_lookup_table *
bit_array_interleave_new_lookup_tables(struct mempool *pool, uint8_t dim)
{
	assert(dim > 0);
	/* Allocate memory for structure */
	struct bit_array_interleave_lookup_table *table = calloc(1,
															 sizeof(struct bit_array_interleave_lookup_table));

	if (table == NULL) {
		return NULL;
	}
	table->dim = dim;
	table->pool = pool;

	/* Allocate memory for buffer */
	table->buffer = bit_array_create(pool, dim);
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

	for(uint8_t i = 0; i < dim; i++) {
		table->tables[i] = calloc(LOOKUP_TABLE_SIZE, sizeof(bit_array**));
		if (table->tables[i] == NULL) {
			bit_array_interleave_free_lookup_tables(table);
			return NULL;
		}

		for (uint16_t j = 0; j < LOOKUP_TABLE_SIZE; j++) {
			table->tables[i][j] = bit_array_create(pool, dim);
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
		for (uint8_t i = 0; i < table->dim; i++) {
			bit_array_interleave_free_dim_lookup_table(table->pool,
													   table->tables[i]);
			table->tables[i] = NULL;
		}

		free(table->tables);
		table->tables = NULL;
	}

	if (table->buffer != NULL) {
		mempool_free(table->pool, table->buffer);
		table->buffer = NULL;
	}

	table->dim = 0;
	free(table);
}

int
bit_array_interleave(struct bit_array_interleave_lookup_table *table,
					 const uint64_t *in, bit_array *out)
{
	const uint8_t octets_count_in_word = 8;
	const uint8_t octet_size_in_bits = 8;
	const uint8_t dim = table->dim;

	bit_array_clear_all(table->buffer, dim);
	for (uint8_t i = 0; i < octets_count_in_word; i++) {
		uint16_t shift = octet_size_in_bits * i;
		for (uint8_t j = 0; j < dim; j++) {
			uint8_t octet = (in[j] >> shift);
			const bit_array *value = table->tables[j][octet];
			bit_array_or(table->buffer, value, dim);
		}
		bit_array_shift_left(table->buffer, dim * shift, dim);
		bit_array_or(out, table->buffer, dim);
		bit_array_clear_all(table->buffer, dim);
	}

	return 0;
}