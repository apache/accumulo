#! /bin/bash

loc=`dirname "$0"`
loc=`cd "$loc/../.."; pwd`

cd "$loc"

fail() {
  echo '   ' $@
  exit 1
}

run() {
  echo $@
  eval $@
  if [ $? -ne 0 ]
  then
    fail $@ fails
  fi
}

runAt() {
  ( cd $1 ; echo in `pwd`; shift ; run $@ ) || fail 
}

run mvn -U -P distclean clean package javadoc:aggregate javadoc:jar source:jar
runAt ./src/server/src/main/c++ make 
run mvn package source:jar assembly:single
run mvn -N rpm:rpm
