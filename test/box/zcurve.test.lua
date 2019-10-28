-------------------------------------------------------------------------------
-- can't be unique
-------------------------------------------------------------------------------
space = box.schema.space.create('uint', { engine = 'memtx' })
pk = space:create_index('primary', { type = 'zcurve', parts = {{1, 'unsigned'}}, unique = true})
space:drop()
space = nil
pk = nil

-------------------------------------------------------------------------------
-- single-part (unsigned) crud
-------------------------------------------------------------------------------

space = box.schema.space.create('uint', { engine = 'memtx' })
pk = space:create_index('primary', { type = 'tree', parts = {{1, 'unsigned'}}})
sk = space:create_index('secondary', { type = 'zcurve', parts = {{2, 'unsigned'}}})

space:replace({0, 0})
space:replace({9, 9})
sk:select({0})
sk:select({9})
pk:update({0}, {{'+', 2, 1}})
pk:update({9}, {{'+', 2, 1}})
sk:select({0})
sk:select({1})
sk:select({9})
sk:select({10})
sk:select()
space:delete({0})
sk:select({0})
space:delete({9})
sk:select()

space:drop()
space = nil
pk = nil
sk = nil

-------------------------------------------------------------------------------
-- single-part (unsigned)
-------------------------------------------------------------------------------

space = box.schema.space.create('uint', { engine = 'memtx' })
pk = space:create_index('primary', { type = 'tree', parts = {{1, 'unsigned'}}, unique = true})
sk = space:create_index('secondary', { type = 'zcurve', parts = {{2, 'unsigned'}}})

for i=1,9 do space:replace{i, i} end

sk:select({}, { iterator = 'ALL' })
sk:select({}, { iterator = 'EQ' })
sk:select({}, { iterator = 'GE' })

sk:select({0}, { iterator = 'EQ' })

sk:select({1}, { iterator = 'EQ' })

sk:select({5}, { iterator = 'EQ' })
sk:select({5}, { iterator = 'GE' })

sk:select({9}, { iterator = 'EQ' })
sk:select({9}, { iterator = 'GE' })

sk:select({10}, { iterator = 'EQ' })
sk:select({10}, { iterator = 'GE' })

space:drop()
space = nil
pk = nil
sk = nil

-------------------------------------------------------------------------------
-- single-part (string)
-------------------------------------------------------------------------------

space = box.schema.space.create('string', { engine = 'memtx' })
pk = space:create_index('primary', { type = 'tree', parts = {{1, 'unsigned'}}, unique = true})
sk = space:create_index('secondary', { type = 'zcurve', parts = {{2, 'string'}}})

-- Z-order curve uses only first 8 bytes of string as key
space:insert{1, '123456789'}
space:insert{2, '12345678'}
sk:select{'12345678'}
sk:select{'123456780'}

-- Check order
space:replace{1, 'aaa'}
space:replace{2, 'bbb'}
space:replace{3, 'bbba'}
space:replace{4, 'c'}
space:replace{5, 'dddd'}
space:replace{6, 'deee'}
space:replace{7, 'eeed'}
sk:select()
sk:select{'b', 'd'}

space:drop()
space = nil
pk = nil
sk = nil

-------------------------------------------------------------------------------
-- single-part (unsigned) range query
-------------------------------------------------------------------------------

space = box.schema.space.create('uint', { engine = 'memtx' })
pk = space:create_index('primary', { type = 'tree', parts = {{1, 'unsigned'}}, unique = true})
sk = space:create_index('secondary', { type = 'zcurve', parts = {{1, 'unsigned'}}})

for i=1,9 do space:replace{i, i} end

sk:select({3, 5}, { iterator = 'ALL' })
sk:select({3, 5}, { iterator = 'EQ' })
sk:select({3, 5}, { iterator = 'GE' })

space:drop()
space = nil
pk = nil
sk = nil

-------------------------------------------------------------------------------
-- multi-part (unsigned + unsigned)
-------------------------------------------------------------------------------

space = box.schema.space.create('unsigned_unsigned', { engine = 'memtx' })
pk = space:create_index('primary', { type = 'tree', parts = {{1, 'unsigned'}}, unique = true})
sk = space:create_index('secondary', { type = 'zcurve', parts = {{2, 'unsigned'}, {3, 'unsigned'}}})

space:replace{1, 2, 4}
space:replace{2, 2, 5}
space:replace{3, 3, 5}
space:replace{4, 3, 4}

sk:select{}
sk:select{1}

space:drop()
space = nil
pk = nil
sk = nil

-------------------------------------------------------------------------------
-- multi-part (unsigned + unsigned) range query
-------------------------------------------------------------------------------

space = box.schema.space.create('unsigned_unsigned', { engine = 'memtx' })
pk = space:create_index('primary', { type = 'tree', parts = {{1, 'unsigned'}}, unique = true})
sk = space:create_index('secondary', { type = 'zcurve', parts = {{2, 'unsigned'}, {3, 'unsigned'}}})

for i=0,5 do for j=0,5 do space:insert{i * 6 + j, i, j} end end

-- returns all tuples
pk:select{}
-- returns all tuples in z-order
sk:select({2, 3, 3, 5}, {iterator = 'ALL'})
-- (2 <= x <= 3) and (3 <= x <= 5)
sk:select{2, 3, 3, 5}
-- (x == 2) and (y == 3)
sk:select{2, 3}
-- (x == 2) and (y == 3)
sk:select({2, 3}, {iterator = 'EQ'})
-- (x >= 2) and (y >= 3)
sk:select({2, 3}, {iterator = 'GE'})
-- (2 <= x <= 3)
sk:select({2, 3, box.NULL, box.NULL})
-- (3 <= y <= 5)
sk:select({box.NULL, box.NULL, 3, 5})
-- (x >= 2) and (y >= 3)
sk:select({2, box.NULL, 3, box.NULL})
-- (x <= 3) and (y <= 4)
sk:select({box.NULL, 3, box.NULL, 4})

space:drop()
space = nil
pk = nil
sk = nil

-------------------------------------------------------------------------------
-- multi-part (float + integer)
-------------------------------------------------------------------------------

space = box.schema.space.create('float_integer', { engine = 'memtx' })
pk = space:create_index('primary', { type = 'tree', parts = {{1, 'unsigned'}}, unique = true})
sk = space:create_index('secondary', { type = 'zcurve', parts = {{2, 'number'}, {3, 'integer'}}})

for i=0,5 do for j=0,5 do space:insert{i * 6 + j, i + 0.01, j} end end

-- returns all tuples
pk:select{}
-- returns all tuples in z-order
sk:select({2, 3, 3, 5}, {iterator = 'ALL'})
-- (2 <= x <= 3) and (3 <= x <= 5)
sk:select{2, 3, 3, 5}
-- (x == 2.01) and (y == 3)
sk:select{2.01, 3}
-- (x == 2.01) and (y == 3)
sk:select({2.01, 3}, {iterator = 'EQ'})
-- (x >= 2.01) and (y >= 3)
sk:select({2.01, 3}, {iterator = 'GE'})
-- (2 <= x <= 3.01)
sk:select({2, 3.01, box.NULL, box.NULL})
-- (3 <= y <= 5)
sk:select({box.NULL, box.NULL, 3, 5})
-- (x >= 2) and (y >= 3)
sk:select({2, box.NULL, 3, box.NULL})
-- (x <= 3) and (y <= 4)
sk:select({box.NULL, 3, box.NULL, 4})

space:drop()
space = nil
pk = nil
sk = nil

-------------------------------------------------------------------------------
-- multi-part non-unique (float + integer)
-------------------------------------------------------------------------------

space = box.schema.space.create('float_integer', { engine = 'memtx' })
pk = space:create_index('primary', { type = 'tree', parts = {{1, 'unsigned'}}, unique = true})
sk = space:create_index('secondary', { type = 'zcurve', parts = {{2, 'number'}, {3, 'integer'}}, unique = false})

space:insert{1, 2, 3}
space:insert{2, 2, 3}
space:insert{3, 2, 3}
space:insert{4, 3, 4}
space:insert{5, 3, 4}
space:insert{6, 3, 4}
space:insert{7, 3, 5}
space:insert{8, 3, 5}
space:insert{9, 3, 5}

-- returns all tuples
pk:select{}

-- returns tuples in range
sk:select{3, 3, 4, 4}

space:drop()
space = nil
pk = nil
sk = nil

-------------------------------------------------------------------------------
-- nullable fields is prohibited
-------------------------------------------------------------------------------
space = box.schema.space.create('zcurve_nullable', { engine = 'memtx' })
pk = space:create_index('primary', { type = 'tree', parts = {{1, 'unsigned'}}, unique = true})
sk = space:create_index('secondary', { type = 'zcurve', parts = {{2, 'unsigned', is_nullable = true}}, unique = false})

space:drop()
space = nil
pk = nil
sk = nil
