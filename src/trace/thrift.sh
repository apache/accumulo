#! /usr/bin/env bash

thrift0.3 -o target -gen java src/main/thrift/cloudtrace.thrift

mkdir -p src/main/java/cloudtrace/thrift
for f in target/gen-java/cloudtrace/thrift/*
do
  DEST=src/main/java/cloudtrace/thrift/`basename $f`
  if ! cmp -s $f $DEST ; then
     echo cp $f $DEST
     cp $f $DEST
  fi
done
