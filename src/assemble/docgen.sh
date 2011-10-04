#! /usr/bin/env bash
if [ ! -z $HADOOP_HOME ]; then
../../bin/accumulo org.apache.accumulo.core.conf.DefaultConfiguration --generate-doc | grep -v ^STANDALONE > ../../docs/config.html
else
echo HADOOP_HOME is not set
fi
