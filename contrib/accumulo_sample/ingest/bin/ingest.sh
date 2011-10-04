#!/bin/bash


THIS_SCRIPT=`readlink -f $0`
SCRIPT_DIR="${THIS_SCRIPT%/*}"

ACCUMULO_HOME=${ACCUMULO_HOME}
ZOOKEEPER_HOME=${ZOOKEEPER_HOME}

#
# Check ZOOKEEPER_HOME
#
if [[ -z $ZOOKEEPER_HOME ]]; then
	echo "You must set ZOOKEEPER_HOME environment variable"
	exit -1;
else
	for f in $ZOOKEEPER_HOME/zookeeper-*.jar; do
		CLASSPATH=$f
		break
	done	
fi

#
# Check ACCUMULO_HOME
#
if [[ -z $ACCUMULO_HOME ]]; then
	echo "You must set ACCUMULO_HOME environment variable"
	exit -1;
else
	for f in $ACCUMULO_HOME/lib/*.jar; do
		CLASSPATH=${CLASSPATH}:$f
	done	
fi

#
# Add our jars
#
for f in $SCRIPT_DIR/../lib/*.jar; do
	CLASSPATH=${CLASSPATH}:$f  
done

#
# Transform the classpath into a comma-separated list also
#
LIBJARS=`echo $CLASSPATH | sed 's/:/,/g'`


#
# Map/Reduce job
#
JAR=$SCRIPT_DIR/../lib/accumulo-sample-ingest-1.4.0-SNAPSHOT.jar
CONF=$SCRIPT_DIR/../conf/wikipedia.xml
HDFS_DATA_DIR=$1
export HADOOP_CLASSPATH=$CLASSPATH
echo "hadoop jar $JAR ingest.WikipediaIngester -libjars $LIBJARS -conf $CONF -Dwikipedia.input=${HDFS_DATA_DIR}"
hadoop jar $JAR ingest.WikipediaIngester -libjars $LIBJARS -conf $CONF -Dwikipedia.input=${HDFS_DATA_DIR}
