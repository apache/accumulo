# Process this file with autoconf to produce a configure script..
AC_INIT(raccumulo, 1.0, datatactics)

## Most of this taken from Simon Urbanek's configure.ac for rJava

##AC_CANONICAL_BUILD
##AC_CANONICAL_HOST

# find R home and set CC/CFLAGS
: ${R_HOME=`R RHOME`}
if test -z "${R_HOME}"; then
  echo "could not determine R_HOME"
  exit 1
fi
# we attempt to use the same compiler as R did
RBIN="${R_HOME}/bin/R"
R_CC=`"${RBIN}" CMD config CC`
R_CPP=`"${RBIN}" CMD config CPP`
R_CFLAGS=`"${RBIN}" CMD config --cppflags`
R_LDLAGS=`"${RBIN}" CMD config --ldflags`
R_SHAREDFLAGS=`"${RBIN}" CMD config SHLIB_LDFLAGS`
