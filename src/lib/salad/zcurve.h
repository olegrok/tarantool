#ifndef TARANTOOL_ZCURVE_H
#define TARANTOOL_ZCURVE_H

#include <stddef.h>
#include <stdbool.h>
#include <assert.h>
#include <salad/bit_array.h>

typedef BIT_ARRAY z_address;

#if defined(__cplusplus)
extern "C" {
#endif /* defined(__cplusplus) */

z_address* zeros(uint32_t part_count);
z_address* ones(uint32_t part_count);
z_address* get_next_zvalue(const z_address* z_value, const z_address* lower_bound,
						   const z_address* upper_bound, bool inc, bool *in_query_box);

#if defined(__cplusplus)
} /* extern "C" { */
#endif /* defined(__cplusplus) */

#endif //TARANTOOL_ZCURVE_H
