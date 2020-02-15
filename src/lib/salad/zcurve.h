#ifndef TARANTOOL_ZCURVE_H
#define TARANTOOL_ZCURVE_H

#include <stddef.h>
#include <stdbool.h>
#include <assert.h>
#include <salad/bit_array.h>

typedef bit_array z_address;
#define z_value_free bit_array_free
#define z_value_cmp bit_array_cmp
#define z_value_create bit_array_create

enum {
	/** Maximal possible Z-order curve dimension */
	ZCURVE_MAX_DIMENSION = 20
};

z_address*
zeros(struct mempool *pool, uint8_t part_count);

z_address*
ones(struct mempool *pool, uint8_t part_count);

void
get_next_zvalue(const z_address *z_value, const z_address *lower_bound,
		const z_address *upper_bound, z_address *out);

/*
 * Provided a minimum Z-address, a maximum Z-address, and a test Z-address,
 * the isRelevant function tells us whether the test address falls within
 * the query rectangle created by the minimum and maximum Z-addresses.
 */
bool
z_value_is_relevant(const z_address *z_value, const z_address *lower_bound,
		const z_address *upper_bound);

#endif //TARANTOOL_ZCURVE_H
