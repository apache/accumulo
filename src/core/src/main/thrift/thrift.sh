#! /usr/bin/env bash

# test to see if we have thrift installed
VERSION=`thrift -version 2>/dev/null | grep "0.3" |  wc -l`
if [ "$VERSION" -ne 1 ] ; then 
   # Nope: bail
   echo "thrift is not available"
   exit 0
fi
CLOUDTRACE=../trace
THRIFT_ARGS="-I $CLOUDTRACE/src/main/thrift -o target "

mkdir -p target
for f in src/main/thrift/*.thrift 
do
	thrift ${THRIFT_ARGS} --gen java $f
	thrift ${THRIFT_ARGS} --gen py $f
	thrift ${THRIFT_ARGS} --gen rb $f
done
# copy only files that have changed
for d in gc master tabletserver security client/impl data
do
   mkdir -p src/main/java/accumulo/core/$d/thrift >& /dev/null
   for f in target/gen-java/accumulo/core/$d/thrift/* 
   do
      DEST="src/main/java/accumulo/core/$d/thrift/`basename $f`"
      if ! cmp -s ${f} ${DEST} ; then
	echo cp ${f} ${DEST} 
	cp ${f} ${DEST} 
      fi
   done
done
