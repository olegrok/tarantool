-------------------------------------------------------------------------------
-- single-part (unsigned) crud
-------------------------------------------------------------------------------

space = box.schema.space.create('uint', { engine = 'memtx' })
pk = space:create_index('primary', { type = 'zcurve', parts = {{1, 'unsigned'}}, unique = true})

space:replace{0, 0}
space:replace{9, 9}
pk:get({0})
pk:get({9})
pk:update(0, {{'+', 2, 1}})
pk:update({9}, {{'+', 2, 1}})
pk:get({0})
pk:get({9})
pk:select()
space:delete({0})
pk:get({0})
space:delete({9})
pk:select()

space:drop()
space = nil
pk = nil

-------------------------------------------------------------------------------
-- single-part (unsigned)
-------------------------------------------------------------------------------

space = box.schema.space.create('uint', { engine = 'memtx' })
pk = space:create_index('primary', { type = 'zcurve', parts = {{1, 'unsigned'}}, unique = true})

for i=1,9 do space:replace{i} end

pk:select({}, { iterator = 'ALL' })
pk:select({}, { iterator = 'EQ' })
pk:select({}, { iterator = 'GE' })

pk:select({0}, { iterator = 'EQ' })

pk:select({1}, { iterator = 'EQ' })

pk:select({5}, { iterator = 'EQ' })
pk:select({5}, { iterator = 'GE' })

pk:select({9}, { iterator = 'EQ' })
pk:select({9}, { iterator = 'GE' })

pk:select({10}, { iterator = 'EQ' })
pk:select({10}, { iterator = 'GE' })

pk:get({})

pk:get({0})
pk:get({5})
pk:get({10})

pk:get({10, 15})

space:drop()
space = nil
pk = nil

-------------------------------------------------------------------------------
-- single-part (string)
-------------------------------------------------------------------------------

space = box.schema.space.create('string', { engine = 'memtx' })
pk = space:create_index('primary', { type = 'zcurve', parts = {{1, 'string'}}, unique = true})

-- Z-order curve uses only first 8 bytes of string as key
space:insert{'123456789'}
space:insert{'12345678'}
pk:select{'12345678'}
pk:delete{'123456780'}

-- Check order
space:replace{'aaa'}
space:replace{'bbb'}
space:replace{'bbba'}
space:replace{'c'}
space:replace{'dddd'}
space:replace{'deee'}
space:replace{'eeed'}
space:select()
space:select{'b', 'd'}

space:drop()
space = nil
pk = nil

-------------------------------------------------------------------------------
-- single-part (unsigned) range query
-------------------------------------------------------------------------------

space = box.schema.space.create('uint', { engine = 'memtx' })
pk = space:create_index('primary', { type = 'zcurve', parts = {{1, 'unsigned'}}, unique = true})

for i=1,9 do space:replace{i} end

pk:select({3, 5}, { iterator = 'ALL' })
pk:select({3, 5}, { iterator = 'EQ' })
pk:select({3, 5}, { iterator = 'GE' })

space:drop()
space = nil
pk = nil

-------------------------------------------------------------------------------
-- multi-part (unsigned + unsigned)
-------------------------------------------------------------------------------

space = box.schema.space.create('multi', { engine = 'memtx' })
pk = space:create_index('primary', { type = 'zcurve', parts = {{1, 'unsigned'}, {2, 'unsigned'}}, unique = true})

space:replace{2, 4}
space:replace{2, 5}
space:replace{3, 5}
space:replace{3, 4}

space:select{}
space:select{1}

space:drop()
space = nil
pk = nil

-------------------------------------------------------------------------------
-- multi-part (unsigned + unsigned) range query
-------------------------------------------------------------------------------

space = box.schema.space.create('multi', { engine = 'memtx' })
pk = space:create_index('primary', { type = 'tree', parts = {{1, 'unsigned'}}, unique = true})
mk = space:create_index('multi', { type = 'zcurve', parts = {{2, 'unsigned'}, {3, 'unsigned'}}})

for i=0,5 do for j=0,5 do space:insert{i * 6 + j, i, j} end end

-- returns all tuples
pk:select{}
-- returns all tuples in z-order
mk:select({2, 3, 3, 5}, {iterator = 'ALL'})
-- (2 <= x <= 3) and (3 <= x <= 5)
mk:select{2, 3, 3, 5}
-- (x == 2) and (y == 3)
mk:select{2, 3}
-- (x == 2) and (y == 3)
mk:select({2, 3}, {iterator = 'EQ'})
-- (x >= 2) and (y >= 3)
mk:select({2, 3}, {iterator = 'GE'})
-- (2 <= x <= 3)
mk:select({2, 3, box.NULL, box.NULL})
-- (3 <= y <= 5)
mk:select({box.NULL, box.NULL, 3, 5})
-- (x >= 2) and (y >= 3)
mk:select({2, box.NULL, 3, box.NULL})
-- (x <= 3) and (y <= 4)
mk:select({box.NULL, 3, box.NULL, 4})

space:drop()
space = nil
pk = nil

-------------------------------------------------------------------------------
-- multi-part (float + integer)
-------------------------------------------------------------------------------

space = box.schema.space.create('multi', { engine = 'memtx' })
pk = space:create_index('primary', { type = 'tree', parts = {{1, 'unsigned'}}, unique = true})
mk = space:create_index('multi', { type = 'zcurve', parts = {{2, 'number'}, {3, 'integer'}}})

for i=0,5 do for j=0,5 do space:insert{i * 6 + j, i + 0.01, j} end end

-- returns all tuples
pk:select{}
-- returns all tuples in z-order
mk:select({2, 3, 3, 5}, {iterator = 'ALL'})
-- (2 <= x <= 3) and (3 <= x <= 5)
mk:select{2, 3, 3, 5}
-- (x == 2.01) and (y == 3)
mk:select{2.01, 3}
-- (x == 2.01) and (y == 3)
mk:select({2.01, 3}, {iterator = 'EQ'})
-- (x >= 2.01) and (y >= 3)
mk:select({2.01, 3}, {iterator = 'GE'})
-- (2 <= x <= 3.01)
mk:select({2, 3.01, box.NULL, box.NULL})
-- (3 <= y <= 5)
mk:select({box.NULL, box.NULL, 3, 5})
-- (x >= 2) and (y >= 3)
mk:select({2, box.NULL, 3, box.NULL})
-- (x <= 3) and (y <= 4)
mk:select({box.NULL, 3, box.NULL, 4})

space:drop()
space = nil
pk = nil

-------------------------------------------------------------------------------
-- multi-part non-unique (float + integer)
-------------------------------------------------------------------------------

space = box.schema.space.create('multi', { engine = 'memtx' })
pk = space:create_index('primary', { type = 'tree', parts = {{1, 'unsigned'}}, unique = true})
mk = space:create_index('multi', { type = 'zcurve', parts = {{2, 'number'}, {3, 'integer'}}, unique = false})

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
mk:select{3, 3, 4, 4}

space:drop()
space = nil
pk = nil

-------------------------------------------------------------------------------
-- nullable fields is prohibited
-------------------------------------------------------------------------------
space = box.schema.space.create('multi', { engine = 'memtx' })
pk = space:create_index('primary', { type = 'tree', parts = {{1, 'unsigned'}}, unique = true})
mk = space:create_index('multi', { type = 'zcurve', parts = {{2, 'unsigned', is_nullable = true}}, unique = false})

space:drop()
space = nil
pk = nil
