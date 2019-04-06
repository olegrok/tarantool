-------------------------------------------------------------------------------
-- single-part (unsigned)
-------------------------------------------------------------------------------

space = box.schema.space.create('uint', { engine = 'memtx' })
pk = space:create_index('primary', { type = 'zcurve', parts = {1, 'unsigned'}, unique = true})

for i=1,9 do space:replace{i} end

pk:select({}, { iterator = 'ALL' })
pk:select({}, { iterator = 'EQ' })
pk:select({}, { iterator = 'REQ' })
pk:select({}, { iterator = 'GE' })
pk:select({}, { iterator = 'GT' })
pk:select({}, { iterator = 'LE' })
pk:select({}, { iterator = 'LT' })

pk:select({0}, { iterator = 'EQ' })
pk:select({0}, { iterator = 'REQ' })
pk:select({0}, { iterator = 'LE' })
pk:select({0}, { iterator = 'LT' })

pk:select({1}, { iterator = 'EQ' })
pk:select({1}, { iterator = 'REQ' })
pk:select({1}, { iterator = 'LE' })
pk:select({1}, { iterator = 'LT' })

pk:select({5}, { iterator = 'EQ' })
pk:select({5}, { iterator = 'REQ' })
pk:select({5}, { iterator = 'GE' })
pk:select({5}, { iterator = 'GT' })
pk:select({5}, { iterator = 'LE' })
pk:select({5}, { iterator = 'LT' })

pk:select({9}, { iterator = 'EQ' })
pk:select({9}, { iterator = 'REQ' })
pk:select({9}, { iterator = 'GE' })
pk:select({9}, { iterator = 'GT' })

pk:select({10}, { iterator = 'EQ' })
pk:select({10}, { iterator = 'REQ' })
pk:select({10}, { iterator = 'GE' })
pk:select({10}, { iterator = 'GT' })

pk:get({})

pk:get({0})
pk:get({5})
pk:get({10})

pk:get({10, 15})

space:drop()
space = nil
pk = nil

-------------------------------------------------------------------------------
-- multi-part (unsigned + unsigned)
-------------------------------------------------------------------------------

space = box.schema.space.create('multi', { engine = 'memtx' })
pk = space:create_index('primary', { type = 'zcurve', parts = {1, 'unsigned', 2, 'unsigned'}, unique = true})

space:replace{2, 4}
space:replace{2, 5}
space:replace{3, 5}
space:replace{3, 4}

space:select{}

space:drop()
space = nil
pk = nil
