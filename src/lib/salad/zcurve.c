#include "zcurve.h"

#define IS_RELEVANT_MASK_MAXLEN 32U

z_address *
zeros(struct mempool *pool, uint8_t part_count)
{
	z_address *result = bit_array_create(pool, part_count);
	bit_array_clear_all(result, part_count);
	return result;
}

z_address *
ones(struct mempool *pool, uint8_t part_count)
{
	z_address *result = bit_array_create(pool, part_count);
	bit_array_set_all(result, part_count);
	return result;
}

static inline uint16_t
bit_position(uint8_t index_dim, uint8_t dim, uint8_t step)
{
	return index_dim * step + dim;
}

static inline uint8_t
get_dim(uint8_t index_dim, uint16_t bit_position)
{
	return bit_position % index_dim;
}

static inline uint8_t
get_step(uint8_t index_dim, uint16_t bit_position)
{
	return bit_position / index_dim;
}

static inline uint8_t
compare_words(const void *restrict value, const void *restrict lower,
		const void *restrict upper)
{
	const uint8_t *first = value, *second = lower, *third = upper;
	uint8_t result = 0;
	uint8_t len = sizeof(uint64_t) / sizeof(uint8_t);
	for (uint8_t i = len - 1;; i--) {
		if (first[i] == second[i] && first[i] == third[i])
			result++;
		else
			break;
		if (i == 0)
			break;
	}
	return result;
}

bool
z_value_is_relevant(const z_address *restrict z_value,
		const z_address *restrict lower_bound,
		const z_address *restrict upper_bound, uint8_t index_dim)
{
	assert(IS_RELEVANT_MASK_MAXLEN > index_dim);

	uint32_t save_min = 0, save_max = 0;
	const uint32_t is_relevant_mask = (UINT32_MAX >>
			(IS_RELEVANT_MASK_MAXLEN - index_dim));

	uint16_t bp = bit_array_length(index_dim);

	uint64_t bit_cursor = 0;
	uint8_t word_cursor = index_dim;
	uint64_t z_value_word = 0, lower_bound_word = 0, upper_bound_word = 0;
	uint8_t dim = 0;
	do {
		if (dim == 0)
			dim = index_dim;

		if (bit_cursor == 0) {
reset_bit_cursor:
			bit_cursor = 1ULL << 63ULL;
			word_cursor--;
			z_value_word = bit_array_get_word(z_value, word_cursor);
			lower_bound_word = bit_array_get_word(lower_bound, word_cursor);
			upper_bound_word = bit_array_get_word(upper_bound, word_cursor);

			uint8_t skip_bytes = compare_words(&z_value_word,
					&lower_bound_word, &upper_bound_word);
			if (skip_bytes > 0) {
				bp -= skip_bytes * CHAR_BIT;
				if (bp == 0)
					return true;
				if (skip_bytes == sizeof(uint64_t))
					goto reset_bit_cursor;
				bit_cursor >>= skip_bytes * CHAR_BIT;
			}

			dim = get_dim(index_dim, bp) + 1;
		}

		bool z_value_bp = z_value_word & bit_cursor;
		bool lower_bound_bp = lower_bound_word & bit_cursor;
		bool upper_bound_bp = upper_bound_word & bit_cursor;

		bp--;
		dim--;
		bit_cursor >>= 1ULL;

		if (z_value_bp == lower_bound_bp && z_value_bp == upper_bound_bp) {
			continue;
		}

		if (z_value_bp != lower_bound_bp) {
			bool save_min_dim_bit = save_min & (1u << dim);
			if (z_value_bp > lower_bound_bp && save_min_dim_bit == 0) {
				save_min |= (1u << dim);
			} else if (z_value_bp < lower_bound_bp && save_min_dim_bit == 0) {
				return false;
			}
		}

		if (z_value_bp != upper_bound_bp) {
			const bool save_max_dim_bit = save_max & (1u << dim);
			if (z_value_bp < upper_bound_bp && save_max_dim_bit == 0) {
				save_max |= (1u << dim);
			} else if (z_value_bp > upper_bound_bp && save_max_dim_bit == 0) {
				return false;
			}
		}

		if (save_max == is_relevant_mask && save_min == is_relevant_mask) {
			break;
		}
	} while (bp > 0);
	return true;
}

void
get_next_zvalue(const z_address *restrict z_value,
		const z_address *restrict lower_bound,
		const z_address *restrict upper_bound,
		z_address *restrict out, uint8_t index_dim)
{
	bit_array_copy(out, z_value, index_dim);
	const uint16_t key_len = bit_array_length(index_dim);

	int8_t flag[index_dim], out_step[index_dim];
	int8_t save_min[index_dim], save_max[index_dim];

	memset(flag, 0, index_dim);
	memset(out_step, INT8_MIN, index_dim);
	memset(save_min, -1, index_dim);
	memset(save_max, -1, index_dim);

	uint32_t is_relevant_mask = (UINT32_MAX >>
			(IS_RELEVANT_MASK_MAXLEN - index_dim));
	uint32_t save_min_mask = 0, save_max_mask = 0;
	uint16_t bp = key_len;

	uint64_t bit_cursor = 0;
	uint64_t z_value_word = 0, lower_bound_word = 0, upper_bound_word = 0;
	uint8_t word_cursor = index_dim;
	do {
		if (bit_cursor == 0) {
reset_bit_cursor:
			bit_cursor = 1ULL << 63ULL;
			word_cursor--;
			z_value_word = bit_array_get_word(z_value, word_cursor);
			lower_bound_word = bit_array_get_word(lower_bound, word_cursor);
			upper_bound_word = bit_array_get_word(upper_bound, word_cursor);

			uint8_t skip_bytes = compare_words(&z_value_word,
					&lower_bound_word, &upper_bound_word);
			if (skip_bytes > 0) {
				bp -= skip_bytes * CHAR_BIT;
				if (bp == 0)
					break;
				if (skip_bytes == sizeof(uint64_t))
					goto reset_bit_cursor;
				bit_cursor >>= skip_bytes * CHAR_BIT;
			}
		}

		const bool z_value_bp = z_value_word & bit_cursor;
		const bool lower_bound_bp = lower_bound_word & bit_cursor;
		const bool upper_bound_bp = upper_bound_word & bit_cursor;
		bit_cursor >>= 1ULL;
		bp--;

		if (z_value_bp == lower_bound_bp && z_value_bp == upper_bound_bp) {
			continue;
		}

		const uint8_t dim = get_dim(index_dim, bp);
		const uint8_t step = get_step(index_dim, bp);

		if (z_value_bp > lower_bound_bp) {
			if (save_min[dim] == -1) {
				save_min_mask |= (1u << dim);
				save_min[dim] = step;
			}
		} else if (z_value_bp < lower_bound_bp) {
			if (flag[dim] == 0 && save_min[dim] == -1) {
				out_step[dim] = step;
				flag[dim] = -1;
			}
		}

		if (z_value_bp < upper_bound_bp) {
			if (save_max[dim] == -1) {
				save_max_mask |= (1u << dim);
				save_max[dim] = step;
			}
		} else if (z_value_bp > upper_bound_bp) {
			if (flag[dim] == 0 && save_max[dim] == -1) {
				out_step[dim] = step;
				flag[dim] = 1;
			}
		}

		if (save_max_mask == is_relevant_mask &&
			save_min_mask == is_relevant_mask) {
			break;
		}
	} while (bp != 0);

	uint8_t max_dim = 0;
	int8_t max_out_step = -1;

	uint8_t i = index_dim;
	do {
		i--;
		if (max_out_step < out_step[i]) {
			max_out_step = out_step[i];
			max_dim = i;
		}
	} while (i != 0);

	uint16_t max_bp = bit_position(index_dim, max_dim, max_out_step);
	if (flag[max_dim] == 1) {
		for (uint16_t new_bp = max_bp + 1; new_bp < key_len; ++new_bp) {
			if (get_step(index_dim, new_bp) <= save_max[get_dim(index_dim, new_bp)] &&
				bit_array_get(z_value, new_bp) == 0) {
				max_bp = new_bp;
				break;
			}
		}
		/* some attributes have to be updated for further processing */
		uint8_t max_bp_dim = get_dim(index_dim, max_bp);
		save_min[max_bp_dim] = get_step(index_dim, max_bp);
		flag[max_bp_dim] = 0;
	}

	for (uint8_t dim = 0; dim < index_dim; ++dim) {
		if (flag[dim] >= 0) {
			/* nip has not fallen below the minimum in dim */
			if (max_bp <= bit_position(index_dim, dim, save_min[dim])) {
				/*
				 * set all bits in dimension dim with
				 * bit position < max_bp to 0 because nip
				 * would not surely get below the lower_bound
				 */
				for (uint16_t bit_pos = dim; bit_pos < max_bp;
					 bit_pos += index_dim) {
					bit_array_clear(out, bit_pos);
				}
			} else {
				/*
				 * set all bits in dimension dim with
				 * bit position < max_bp to the value of
				 * corresponding bits of the lower_bound
				 */
				for (uint16_t bit_pos = dim; bit_pos < max_bp;
					 bit_pos += index_dim) {
					bit_array_assign(out, bit_pos,
										 bit_array_get(lower_bound, bit_pos));
				}
			}
		} else {
			/*
			 * nip has fallen below the minimum in dim
			 * set all bits in dimension dim to the value of
			 * corresponding bits of the lower_bound because the minimum would not
			 * be exceeded otherwise
			 */
			for (bp = dim; bp < key_len; bp += index_dim) {
				bit_array_assign(out, bp, bit_array_get(lower_bound, bp));
			}
		}
	}

	bit_array_set(out, max_bp);
}
