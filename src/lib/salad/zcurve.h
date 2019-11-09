#ifndef TARANTOOL_ZCURVE_H
#define TARANTOOL_ZCURVE_H

#include <stddef.h>
#include <stdbool.h>
#include <assert.h>
#include <salad/bit_array.h>

typedef bit_array z_address;
#define z_value_free bit_array_free
#define z_value_cmp bit_array_cmp

enum {
	/** Maximal possible Z-order curve dimension */
	ZCURVE_MAX_DIMENSION = 20
};

#if defined(__cplusplus)
extern "C" {
#endif /* defined(__cplusplus) */

z_address*
zeros(uint32_t part_count);

z_address*
ones(uint32_t part_count);

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

#if defined(__cplusplus)
} /* extern "C" { */
#endif /* defined(__cplusplus) */

#endif //TARANTOOL_ZCURVE_H
