AC_INIT([tmpcache],[0],[matthewaeverett@googlemail.com])
AM_INIT_AUTOMAKE([foreign -Wall -Werror])
AC_PROG_CC
AC_CHECK_HEADERS([xs/xs.h cdb.h])
AC_CHECK_LIB([xs],[xs_init])
AC_CHECK_LIB([cdb],[cdb_init])
AC_CHECK_LIB([pthread],[pthread_create])
AC_CONFIG_HEADERS([config.h])
AC_CONFIG_FILES([Makefile src/Makefile])
AC_OUTPUT
