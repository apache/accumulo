#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.



THIS_SCRIPT="$0"
SCRIPT_DIR="${THIS_SCRIPT%/*}"
SCRIPT_DIR=`cd $SCRIPT_DIR ; pwd`
echo $SCRIPT_DIR

#
# Add our jars
#
for f in $SCRIPT_DIR/../lib/*.jar; do
	CLASSPATH=${CLASSPATH}:$f  
done

#
# Transform the classpath into a comma-separated list also
#
LIBJARS=`echo $CLASSPATH | sed 's/^://' | sed 's/:/,/g'`


#
# Map/Reduce job
#
JAR=$SCRIPT_DIR/../lib/wikisearch-ingest-1.5.0-SNAPSHOT.jar
CONF=$SCRIPT_DIR/../conf/wikipedia.xml
HDFS_DATA_DIR=$1
export HADOOP_CLASSPATH=$CLASSPATH
echo "hadoop jar $JAR org.apache.accumulo.examples.wikisearch.ingest.WikipediaPartitionedIngester -libjars $LIBJARS -conf $CONF -Dwikipedia.input=${HDFS_DATA_DIR}"
hadoop jar $JAR org.apache.accumulo.examples.wikisearch.ingest.WikipediaPartitionedIngester -libjars $LIBJARS -conf $CONF -Dwikipedia.input=${HDFS_DATA_DIR}
