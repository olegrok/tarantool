# - Find the readline include files and libraries
# - Include finding of termcap or curses
#
# READLINE_FOUND
# READLINE_INCLUDE_DIR
# READLINE_LIBRARIES
#

if(BUILD_STATIC)
    find_library(CURSES_CURSES_LIBRARY NAMES libcurses.a)
    find_library(CURSES_NCURSES_LIBRARY NAMES libncurses.a)
    find_library(CURSES_FORM_LIBRARY NAMES libform.a)
    find_library(CURSES_INFO_LIBRARY NAMES libtinfo.a)
    if (NOT CURSES_INFO_LIBRARY)
	set(CURSES_INFO_LIBRARY "")
    endif()
endif()
find_package(Curses)
if(NOT CURSES_FOUND)
    find_package(Termcap)
endif()

set(READLINE_STATIC libreadline.a)

if (DEFINED READLINE_ROOT)
  set(_FIND_OPTS NO_CMAKE NO_CMAKE_SYSTEM_PATH)
  find_library(READLINE_LIBRARY
    NAMES ${READLINE_STATIC} readline
    HINTS ${READLINE_ROOT}/lib
    ${_FIND_OPTS})
  find_path(READLINE_INCLUDE_DIR
    NAMES readline/readline.h
    HINTS ${READLINE_ROOT}/include ${_FIND_OPTS})
else()
    find_library(READLINE_LIBRARY NAMES ${READLINE_STATIC} readline)
  find_path(READLINE_INCLUDE_DIR readline/readline.h)
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Readline
    REQUIRED_VARS READLINE_INCLUDE_DIR READLINE_LIBRARY)
set(READLINE_INCLUDE_DIRS ${READLINE_INCLUDE_DIR})
set(READLINE_LIBRARIES ${READLINE_LIBRARY})

if(READLINE_FOUND)
  if(EXISTS ${READLINE_INCLUDE_DIR}/readline/rlconf.h)
      check_library_exists(${READLINE_LIBRARY} rl_catch_sigwinch ""
          HAVE_GNU_READLINE)
      if(HAVE_GNU_READLINE)
          find_package_message(GNU_READLINE "Detected GNU Readline"
              "${HAVE_GNU_READLINE}")
      endif()
  endif()
  if(CURSES_FOUND)
    set(READLINE_LIBRARIES ${READLINE_LIBRARIES} ${CURSES_LIBRARIES} ${CURSES_INFO_LIBRARY})
    set(READLINE_INCLUDE_DIRS ${READLINE_INCLUDE_DIRS} ${CURSES_INCLUDE_DIRS})
  elseif(TERMCAP_FOUND)
    set(READLINE_LIBRARIES ${READLINE_LIBRARIES} ${TERMCAP_LIBRARIES} ${CURSES_INFO_LIBRARY})
    set(READLINE_INCLUDE_DIRS ${READLINE_INCLUDE_DIRS} ${TERMCAP_INCLUDE_DIRS})
  endif()
endif(READLINE_FOUND)

mark_as_advanced(READLINE_INCLUDE_DIRS READLINE_LIBRARIES)
