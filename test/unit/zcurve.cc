#include "unit.h"
#include "salad/zcurve.h"
#include "salad/bit_array.h"

const size_t key_size_in_bits = 64lu;

static z_address *create_key2d_from_number(uint64_t num) {
	z_address *key = bit_array_create(2 * key_size_in_bits);
	bit_array_clear_all(key);
	bit_array_add_uint64(key, num);
	return key;
}

static void next_jump_in_check_2d() {
	size_t test_plan = 19;
	plan(2 * test_plan);
	header();
	struct test_case {
		uint64_t test_point;
		uint64_t expected;
	};

	uint64_t lower_bound = 11;
	uint64_t upper_bound = 50;
	struct test_case test_cases[] {
			{ 11, 11 },
			{ 12, 14 },
			{ 13, 14 },
			{ 14, 14 },
			{ 16, 26 },
			{ 17, 26 },
			{ 18, 26 },
			{ 19, 26 },
			{ 20, 26 },
			{ 25, 26 },
			{ 26, 26 },
			{ 27, 33 },
			{ 32, 33 },
			{ 33, 33 },
			{ 34, 35 },
			{ 35, 35 },
			{ 40, 48 },
			{ 49, 50 },
			{ 50, 50 },
	};

	z_address *z_lower_bound = create_key2d_from_number(lower_bound);
	z_address *z_upper_bound = create_key2d_from_number(upper_bound);

	for (size_t i = 0; i < test_plan; ++i) {
		z_address *value = create_key2d_from_number(test_cases[i].test_point);
		z_address *expected = create_key2d_from_number(test_cases[i].expected);
		bool in_query_box;
		z_address *result = get_next_zvalue(value, z_lower_bound,
				z_upper_bound, &in_query_box);
		is(bit_array_cmp(result, expected), 0, "%" PRIu64 " -> %" PRIu64,
				test_cases[i].test_point, test_cases[i].expected);
		bool test_case_in_qb = test_cases[i].test_point == test_cases[i].expected;
		is(test_case_in_qb, in_query_box,
				test_case_in_qb ? "in query box" : "not in query box")
		bit_array_free(result);
		bit_array_free(value);
		bit_array_free(expected);
	}

	bit_array_free(z_lower_bound);
	bit_array_free(z_upper_bound);
	footer();
}

int main() {
	plan(1);
	next_jump_in_check_2d();
	return 0;
}
