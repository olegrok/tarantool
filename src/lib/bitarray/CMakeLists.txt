set(lib_sources bit_array.c)
set_source_files_compile_flags(${lib_sources})
add_library(bitarray STATIC ${lib_sources})
target_link_libraries(bitarray)
