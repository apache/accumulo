#! /usr/bin/env bash
# a little helper script that other scripts can source to setup 
# for running a map reduce job

. continuous-env.sh
. $ACCUMULO_HOME/conf/accumulo-env.sh

SERVER_CMD='ls -1 $ACCUMULO_HOME/lib/accumulo-server-*[!javadoc\|sources].jar'

if [ `eval $SERVER_CMD | wc -l` != "1" ] ; then
    echo "Not exactly one accumulo-server jar in $ACCUMULO_HOME/lib"
    exit 1
fi

SERVER_LIBJAR=`eval $SERVER_CMD`
