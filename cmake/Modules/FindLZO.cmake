#-------------------------------------------------------------------------------
#  cb2bib cmake lzo find module
#-------------------------------------------------------------------------------
#  Modified FindLZO.cmake module from the LGPL 3.0 project blobseer
#  http://blobseer.gforge.inria.fr/doku.php
#-------------------------------------------------------------------------------
#  Find liblzo2
#  LZO_FOUND - system has LZO2 library
#  LZO_INCLUDE_DIR - LZO2 include directory
#  LZO_LIBRARIES - The libraries needed to use LZO2
#-------------------------------------------------------------------------------

if (LZO_INCLUDE_DIR AND LZO_LIBRARIES)

  # in cache already
  SET(LZO_FOUND TRUE)

else (LZO_INCLUDE_DIR AND LZO_LIBRARIES)
  FIND_PATH(LZO_INCLUDE_DIR lzo/lzoconf.h
     ${LZO_ROOT}/include/lzo/
     /usr/include/lzo/
     /usr/local/include/lzo/
     /sw/lib/lzo/
     /sw/local/lib/lzo/
  )

  if(WIN32 AND MSVC)
  else(WIN32 AND MSVC)
    FIND_LIBRARY(LZO_LIBRARIES NAMES lzo2
      PATHS
      ${LZO_ROOT}/lib
      /sw/lib
      /sw/local/lib
      /usr/lib
      /usr/local/lib
    )
  endif(WIN32 AND MSVC)

  if (LZO_INCLUDE_DIR AND LZO_LIBRARIES)
     set(LZO_FOUND TRUE)
  endif (LZO_INCLUDE_DIR AND LZO_LIBRARIES)

  if (LZO_FOUND)
     if (NOT LZO_FIND_QUIETLY)
        message(STATUS "Found LZO2: ${LZO_LIBRARIES}")
     endif (NOT LZO_FIND_QUIETLY)
  else (LZO_FOUND)
     if (LZO_FIND_REQUIRED)
        message(FATAL_ERROR "Could NOT find LZO2")
     endif (LZO_FIND_REQUIRED)
  endif (LZO_FOUND)

  MARK_AS_ADVANCED(LZO_INCLUDE_DIR LZO_LIBRARIES)

endif (LZO_INCLUDE_DIR AND LZO_LIBRARIES)

#-------------------------------------------------------------------------------