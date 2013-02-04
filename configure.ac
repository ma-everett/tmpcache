AC_INIT([tmpcache],[0],[matthewaeverett@googlemail.com])
AM_INIT_AUTOMAKE([foreign -Wall -Werror  dist-bzip2])
AC_PROG_CC
AC_CHECK_LIB([zmq],[zmq_ctx_new])
AC_CHECK_LIB([cdb],[cdb_init])
AC_CHECK_LIB([pthread],[pthread_create])
AC_CHECK_LIB([argtable2],[arg_parse])
AC_CONFIG_HEADERS([config.h])
AC_CONFIG_FILES([Makefile src/Makefile])
AC_OUTPUT
