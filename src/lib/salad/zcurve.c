#include "zcurve.h"

#define KEY_SIZE_IN_BITS (64lu)

z_address* zeros(uint32_t part_count) {
	z_address* result = bit_array_create(part_count * KEY_SIZE_IN_BITS);
	bit_array_clear_all(result);
	return result;
}

z_address* ones(uint32_t part_count) {
	z_address* result = bit_array_create(part_count * KEY_SIZE_IN_BITS);
	bit_array_set_all(result);
	return result;
}

static size_t bit_position(size_t index_dim, uint32_t dim, uint8_t step) {
	return index_dim * step + dim;
}

static uint32_t get_dim(uint32_t index_dim, size_t bit_position) {
	return bit_position % index_dim;
}

static uint8_t get_step(uint32_t index_dim, size_t bit_position) {
	return bit_position / index_dim;
}

bool z_value_is_relevant(const z_address* z_value, const z_address* lower_bound,
				 const z_address* upper_bound) {
	const size_t key_len = bit_array_length(z_value);
	assert(key_len == bit_array_length(lower_bound));
	assert(key_len == bit_array_length(upper_bound));
	const uint32_t index_dim = key_len / KEY_SIZE_IN_BITS;

	int32_t save_min[index_dim], save_max[index_dim];

	for (uint32_t i = 0; i < index_dim; ++i) {
		save_min[i] = -1;
		save_max[i] = -1;
	}

	size_t bp = key_len;
	do {
		bp--;
		uint32_t dim = get_dim(index_dim, bp);
		uint8_t step = get_step(index_dim, bp);

		if (bit_array_get_bit(z_value, bp) > bit_array_get_bit(lower_bound, bp)) {
			if (save_min[dim] == -1) {
				save_min[dim] = step;
			}
		} else if (bit_array_get_bit(z_value, bp) < bit_array_get_bit(lower_bound, bp)) {
			if (save_min[dim] == -1) {
				return false;
			}
		}

		if (bit_array_get_bit(z_value, bp) < bit_array_get_bit(upper_bound, bp)) {
			if (save_max[dim] == -1) {
				save_max[dim] = step;
			}
		} else if (bit_array_get_bit(z_value, bp) > bit_array_get_bit(upper_bound, bp)) {
			if (save_max[dim] == -1) {
				return false;
			}
		}
	} while (bp > 0);
	return true;
}

z_address* get_next_zvalue(const z_address* z_value, const z_address* lower_bound,
						   const z_address* upper_bound, bool *in_query_box) {
	const size_t key_len = bit_array_length(z_value);
	assert(key_len == bit_array_length(lower_bound));
	assert(key_len == bit_array_length(upper_bound));
	const uint32_t index_dim = key_len / KEY_SIZE_IN_BITS;

	z_address* result = bit_array_clone(z_value);

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
	if (in_query_box != NULL) {
		*in_query_box = is_nip;
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
