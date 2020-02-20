#include "memory.h"
#include "fiber.h"
#include "unit.h"
#include "salad/zcurve.h"
#include "salad/bit_array.h"

static z_address *
create_key2d_from_number(struct mempool *pool, uint8_t dim, uint64_t num)
{
	z_address *key = bit_array_create(pool, dim);
	bit_array_add_word(key, num, dim);
	return key;
}

static void
next_jump_in_check_2d() {
	word_size_t dim = 2;
	struct mempool pool = {};
	mempool_create(&pool, cord_slab_cache(),
				   bit_array_bsize(dim));

    struct test_case {
        uint64_t test_point;
        uint64_t expected;
    };

	header();
	uint64_t lower_bound = 11;
	uint64_t upper_bound = 50;
	struct test_case test_cases[] = {
			{ 12, 14 },
			{ 13, 14 },
			{ 16, 26 },
			{ 17, 26 },
			{ 18, 26 },
			{ 19, 26 },
			{ 20, 26 },
			{ 25, 26 },
			{ 27, 33 },
			{ 32, 33 },
			{ 34, 35 },
			{ 40, 48 },
			{ 49, 50 },
#ifndef NDEBUG
/*
 * For performance reasons border points are disabled
 * in not debug mode.
 * For details read a comment in get_next_zvalue function.
 */
			{ 11, 11 },
			{ 14, 14 },
			{ 26, 26 },
			{ 33, 33 },
			{ 35, 35 },
			{ 50, 50 },
#endif
	};
	size_t test_plan = sizeof(test_cases) / sizeof(struct test_case);
	plan(test_plan);

	z_address *z_lower_bound = create_key2d_from_number(&pool, dim,
			lower_bound);
	z_address *z_upper_bound = create_key2d_from_number(&pool, dim,
			upper_bound);
	z_address *result = bit_array_create(&pool, dim);

	for (size_t i = 0; i < test_plan; ++i) {
		z_address *value = create_key2d_from_number(&pool, dim,
				test_cases[i].test_point);
		z_address *expected = create_key2d_from_number(&pool, dim,
				test_cases[i].expected);
		get_next_zvalue(value, z_lower_bound, z_upper_bound, result, dim);
		is(bit_array_cmp(result, expected, dim), 0, "%" PRIu64 " -> %" PRIu64,
				test_cases[i].test_point, test_cases[i].expected);
		bit_array_free(&pool, value);
		bit_array_free(&pool, expected);
	}

	bit_array_free(&pool, result);
	bit_array_free(&pool, z_lower_bound);
	bit_array_free(&pool, z_upper_bound);
	mempool_destroy(&pool);
	footer();
    check_plan();
}

static void
is_relevant_check_2d() {
	word_size_t dim = 2;
	struct mempool pool = {};
	mempool_create(&pool, cord_slab_cache(),
				   bit_array_bsize(dim));

    size_t test_plan = 14;
    header();
    plan(test_plan);

    struct test_case {
        uint64_t test_point;
        bool expected;
    };

    uint64_t lower_bound = 4;
    uint64_t upper_bound = 51;
    struct test_case test_cases[] = {
            { 0, false },
            { 4, true },
            { 7, true },
            { 8, false },
            { 11, false },
            { 12, true },
            { 19, true },
            { 20, false },
            { 23, false },
            { 24, true },
            { 35, false },
            { 47, false },
            { 51, true },
            { 52, false },
    };

    z_address *z_lower_bound = create_key2d_from_number(&pool, dim,
    		lower_bound);
    z_address *z_upper_bound = create_key2d_from_number(&pool, dim,
    		upper_bound);

    for (size_t i = 0; i < test_plan; ++i) {
        z_address *value = create_key2d_from_number(&pool, dim,
        		test_cases[i].test_point);
        bool is_relevant = z_value_is_relevant(value, z_lower_bound,
                z_upper_bound, dim);
        is(is_relevant, test_cases[i].expected,
                is_relevant ? "in query box" : "not in query box");
        bit_array_free(&pool, value);
    }

    bit_array_free(&pool, z_lower_bound);
    bit_array_free(&pool, z_upper_bound);
	mempool_destroy(&pool);
    footer();
    check_plan();
}

static void
zcurve_C_test_init()
{
	/* Suppress info messages. */
	say_set_log_level(S_WARN);

	memory_init();
	fiber_init(fiber_c_invoke);
}

int main() {
	zcurve_C_test_init();

	next_jump_in_check_2d();
    is_relevant_check_2d();
	return 0;
}
