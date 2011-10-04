#! /usr/bin/env bash

fail() {
   echo $@
   exit 1
}

# test to see if we have thrift installed
VERSION=`thrift -version 2>/dev/null | grep "0.6" |  wc -l`
if [ "$VERSION" -ne 1 ] ; then 
   # Nope: bail
   echo "thrift is not available"
   exit 0
fi
CLOUDTRACE=../../contrib/cloudtrace-0.2.0/
test -d ${CLOUDTRACE} || ( echo 'need to configure cloudtrace' ; exit 0 )

THRIFT_ARGS="-I $CLOUDTRACE/src/main/thrift -o target "

mkdir -p target
for f in src/main/thrift/*.thrift 
do
	thrift ${THRIFT_ARGS} --gen java $f || fail unable to generate java thrift classes
	thrift ${THRIFT_ARGS} --gen py $f || fail unable to generate python thrift classes
	thrift ${THRIFT_ARGS} --gen rb $f || fail unable to generate ruby thrift classes
done
# copy only files that have changed
for d in gc master tabletserver security client/impl data
do
   mkdir -p src/main/java/org/apache/accumulo/core/$d/thrift >& /dev/null
   for f in target/gen-java/org/apache/accumulo/core/$d/thrift/* 
   do
      DEST="src/main/java/org/apache/accumulo/core/$d/thrift/`basename $f`"
      if ! cmp -s ${f} ${DEST} ; then
	echo cp ${f} ${DEST} 
	cp ${f} ${DEST} || fail unable to copy files to java workspace
      fi
   done
done
