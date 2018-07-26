# Tarantool JSON Indexes

* **Status**: In progress
* **Start date**: 26-07-2018
* **Authors**: Vladimir Davydov @locker vdavydov.dev@gmail.com, Vladislav Shpilevoy @Gerold103 v.shpilevoy@tarantool.org, Kirill Shcherbatov @kshcherbatov kshcherbatov@tarantool.org
* **Issues**: [#1012](https://github.com/tarantool/tarantool/issues/1012)

## Summary
Tarantool *JSON Indexes* is a feature that allows to index space's tuples that have consistent document-type complex structure.


## Background and motivation
The box.index submodule provides read-only access for index definitions and index keys. Indexes are contained in box.space.space-name.index array within each space object. They provide an API for ordered iteration over tuples. This API is a direct binding to corresponding methods of index objects of type box.index in the storage engine.
```
s:create_index('primary', {parts = {1, 'unsigned'}, {2, 'str'}})
```

Sometimes tuple space data have complex but consistent document structure.
```
s:insert{1, {town = 'NY', name = {first = 'Richard', second = 'Feynman'}}}
s:insert{2, {town = 'NY', name = {first = 'Ayn', second = 'Rand'}}}
```
Building index on document fields would be attractive feature that allows to search and iterate over such data.
```
s:create_index('name'', { parts = {{ "name.first", "str" }, {"name.last", "str"}}})
```

Fields having complex document structure should have 'map' or 'array' type in format if specified.


## Detailed design
All data in Tarantool stored in atomic database objects called *tuples*. They consist of payload(user data represented as *msgpack*) and extra information that describe how does database should operate with it.
```
  [[ Img 0.1 - Tarantool tuple structure ]]

                           +----------------------+-----------------------+-------------+
  tuple_begin, ..., raw =  |      extra_size      | offset N ... offset 1 | MessagePack |
  |                        +----------------------+-----------------------+-------------+
  |                                                                       ^
  +-----------------------------------------------------------------------data_offset
```
Indexed information should be accessible really fast. In case of regular indexes on fields it comes possible with *field_map* - data offsets stored before user payload in each tuple. Metadata cells that are used to store those offsets *offset_slot* are not same for different *formats*(aggregated information of all indexes describing specific space). Without loss of generality this mean that tuples constructed at the moment 'A' when only PK index was registered have another *format* than the tuples constructed at the moment 'B' after new Index "name" was added.

The concept idea with *offset_slot*s could be used for JSON Indexes too.

JSON-path definitely would be a part of *key_part* structure. As we have already noted, *offset_slots* are different for formats of different generations, so we should find an universal and fast way to obtain it by JSON. As code that access tuple data by index is a really hot, the performance is our priority problem.
We already have *tuple_field_raw_by_path* that follows JSON path in tuple and returns pointer to requested field. Sometimes fields that nested in JSON-structured document may have coinciding path part:
e.g.:
```
s:create_index('name'', { parts = {{ "name.first", "str" }, {"name.last", "str"}}})
s:create_index('name'', { parts = {{ "town", "str" }, {"name.last", "str"}}})
```
We don't like to parse coinciding suffixes multiple time for same tuples.

### 1.1. JSON-path tree in tuple format
Keeping in mind all challenges we suppose to introduce tree-based structure in *tuple_format* that contain JSON-path lexemes and has allocated tuple *offset_slots* as leaves nodes.
```
   [[ Img 1.1 - JSON-path tree in tuple format ]]

   ----\
       |-----> [name] \
       |              |----> [first]   <slot1 = -3>
       |              |
       |              |----> [last]    <slot2 = -1>
       |
       \------> [town]    <slot3 = -1>
```

For example, when first index part "name.first" is built, we would already have name offset and would able to start there parsing only "last" lexeme.
```
s:create_index('name'', { parts = {{ "name.first", "str" },
                                   { "name.last",  "str" }}})
```
The resulting information offset would be stored in slot obtained from tree leaf node.

```
    [[ Img 1.2 - Tuple data stored in database of same format ]]

                                                              |  Description  |

     {-3}    {-2}    {-1}                                     |   slot index  |
   +=======+=======+=======+======+===================+
   | slot1 | slot2 | slot3 | town |       name        |       |  tuple schema |
   +=======+=======+=======+======+=====+======+======+
   |   2   |   4   |   0   | NY   | Ayn | Rand |              |  tuple1 data  |
   +-------+-------+-------+------+-----+---+--+------+
   |   2   |   5   |   0   | NY   | Richard | Feynman |       |  tuple2 data  |
   +-------+-------+-------+------+---------+---------+
                           ^      ^     ^
                           0      2     4                     | tuple1 offset |

                           ^      ^         ^
                           0      2         5                 | tuple2 offset |
```

### 1.2. Access data by JSON-path
The central API to access information is a routine
```
static inline const char *
tuple_field_raw(const struct tuple_format *format, const char *tuple,
                const uint32_t *field_map, uint32_t field_no)
```

In most scenarios related to indexes it is called with *field_no* extracted from *key_part* - a descriptor of part representing index.
```
field_b = tuple_field_raw(format_b, tuple_b_raw, field_map_b, part->fieldno);
```
With introducing parts containing JSON path we need to rework such calls.

To avoid parsing JSON path each time we suppose to introduce a cache for *offset_slot* in *key_part* structure:
```
struct key_part {
  /** Tuple field index for this part */
  uint32_t fieldno;
  ...

  /** Data JSON path. */
  const char *data_path;
  /** Epoch of tuple_format cached in offset_slot. */
  uint32_t slot_epoch;
  /** Cache of corresponding tuple_format offset_slot. */
  int32_t slot_cache;
};
```
And extend *tuple_format* where such epoch would be initialized with a new bigger(when source key_parts have changed) value on creation.
```
struct tuple_format {
  ...

  /** Epoch of tuple format. */
  uint32_t epoch;
  /** Formats of the fields. */
  struct tuple_field fields[0];
};
```
We would modify tuple_field to have a tree structure if necessary and represent all intimidate records.

In all index-related scenarios with *tuple_field_raw* we would rework with a new function:
```
const char *
tuple_field_by_part(const struct tuple *tuple, const struct key_def *def,
                    uint32_t idx);

```

With first argument *tuple* we can obtain tuple_format that contain *epoch* field.
```
  [[ Img 2.1 - Block diagram for tuple_field_by_part routine ]]

            [[def->epoch == tuple_format(tuple)->epoch]]
                                 ||
                  YES            ||             NO
                 ________________/\________________
                |                                  |

     use offset_slot from             PARSE def->parts[idx]->data_path and
  def->parts[idx].cached_slot     observe tuple_format(tuple)->fields structure,
                                     UPDATE def->parts[idx].slot_epoch and
                               def->parts[idx].slot_cache IF format epoch is bigger
```

PARSE is about parsing JSON path lexeme-by-lexeme and step down the fields tree until leaf with slot offset would reached.
Then we need to UPDATE key_part cache IF epoch has increased. This *may* make epoch oscillations not so significant.

To sped-up data access we suppose to use global format hashtable with all paths:
```
   HASHTABLE<const char *data_path, int32_t offset_slot>

                            lookup
   "[2]name.first" ----> hash1 ------> [hash1, "name.first", offset_slot1]
   "[2]name.last"  ----> hash2 ------> [hash2, "name.last", offset_slot2]
```

## Rationale and alternatives
### 2.1 Reallocatable slot_cache
Epoch-based invalidation is looking complicated enough.
It is possible to allocate slot_cache[] array for each key_part and store slots for all epoch (where epoch would be an index in that array).
```
                         format->epoch
key_part {                     ||
   slot_epoch[]:               \/
        [1: s1, 2: s2, ... epoch: s3, ... max_epoch: sN]
}

I.e.:
    tuple_map[key_part->slot_epoch[format->epoch]]
```
But as formats are created really often, this approach is really resource-consumable.
