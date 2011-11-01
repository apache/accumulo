#! /bin/sh

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

# This script converts an existing installation to the new accumulo name
#   copies data in zookeeper to a new location, with updated class names
#   moves the data in HDFS to prevent any rogue servers from touching the data

# Where are we?
ACCUMULO_HOME=`dirname "$0"`
ACCUMULO_HOME=`cd "$ACCUMULO_HOME/.."; pwd`

PREVIOUS=cloudbase

fail() {
   echo "$@" 1>&2
   exit 1
}

verbose() {
   echo "$@"
}

usage() {
   echo
   echo usage:
   echo
   echo "    $0 <zoohost> <hdfs-before> <hdfs-after>"
   echo
   echo try /$PREVIOUS for hdfs-before, and /accumulo for hdfs-after
   echo
   exit 1
}

# double check that we were given a zookeeper host(s)
ZOOKEEPER="$1"
test -z "$ZOOKEEPER" && usage
DFSBEFORE="$2"
test -z "$DFSBEFORE" && usage
DFSAFTER="$3"
test -z "$DFSAFTER" && usage

# double check that the installation looks reasonable
for normal in bin conf lib
do
  if [ ! -d "$ACCUMULO_HOME/$normal" ] 
  then
    fail Missing $normal in $ACCUMULO_HOME: is ACCUMULO_HOME set properly?
  fi
done

# ensure the user has done some configuration
for mustHave in conf/accumulo-env.sh conf/accumulo-site.xml
do
  if [ ! -f "$ACCUMULO_HOME/$mustHave" ]
  then
   fail You must configure accumulo: create "$mustHave" from your existing install
  fi
done

egrep -q "$PREVIOUS.(core|server)" "$ACCUMULO_HOME"/conf/accumulo-site.xml && fail "$PREVIOUS" found in the conf/accumulo-site.xml file, please rename it manually

EXAMPLE_PROPERTY="

    <property>
      <name>instance.dfs.dir</name>
      <value>$DFSAFTER</value>
    </property>
"

if [ "$DFSAFTER" != "/accumulo" ]
then
   grep -q -e "$DFSAFTER" "$ACCUMULO_HOME/conf/accumulo-site.xml" || fail "You need to define instance.dfs.dir in to be $DFSAFTER in accumulo-site.xml. Add $EXAMPLE_PROPERTY"
fi

. $ACCUMULO_HOME/conf/accumulo-env.sh
test -z "$HADOOP_COMMAND" && HADOOP_COMMAND="$HADOOP_HOME/bin/hadoop"
test -x "$HADOOP_COMMAND" || fail You must set HADOOP_HOME, or set HADOOP_COMMAND to the path for the '"hadoop"' command
test -z "$ZOOKEEPER_COMMAND" && ZOOKEEPER_COMMAND="$ZOOKEEPER_HOME/bin/zkCli.sh"
test -x "$ZOOKEEPER_COMMAND" || fail You must set ZOOKEEPER_HOME, or set ZOOKEEPER_COMMAND to the path for the '"zkCli.sh"' command

verbose Checking pre-conditions 
"$HADOOP_COMMAND" fs -test -e "$DFSAFTER" && fail "$DFSAFTER should not exist"
"$HADOOP_COMMAND" fs -test -e "$DFSBEFORE" || fail "$DFSBEFORE does not exist"

INSTANCE=`"$HADOOP_COMMAND" fs -ls "$DFSBEFORE/instance_id" | awk -F/ '{ print $NF }' | tail -1` || fail "Unable to get instance id"
verbose Found instance "$INSTANCE" in HDFS


VERSION=`"$HADOOP_COMMAND" fs -ls "$DFSBEFORE/version" | awk -F/ '{ print $NF }' | tail -1` || fail "Unable to get the version of the existing installation"
if [ "$VERSION" -ne 3 ]
then
   fail This script only works on $PREVIOUS-1.3.X
fi

verbose Getting instance name
NAME=`"$ACCUMULO_HOME/bin/accumulo" org.apache.accumulo.server.util.DumpZookeeper "$ZOOKEEPER" "/$PREVIOUS/instances" | grep "$INSTANCE" | grep value | sed "s/.*name='\([^']*\).*/\1/" | head -1` || fail "Unable to get instance name"
verbose instance name "$NAME"
verbose Getting zookeeper settings
TEMP=/tmp/accumulo-rename-$$
"$ACCUMULO_HOME"/bin/accumulo org.apache.accumulo.server.util.DumpZookeeper "$ZOOKEEPER" "/$PREVIOUS/$INSTANCE" >"$TEMP"
grep -q ephemeral "$TEMP" && fail There are running programs still using zookeeper.
verbose switching names
sed -i.xml \
 -e s/$PREVIOUS.core.iterators.AggregatingIterator/org.apache.accumulo.core.iterators.AggregatingIterator/g \
 -e s/$PREVIOUS.core.iterators.aggregation.Aggregator/org.apache.accumulo.core.iterators.aggregation.Aggregator/g \
 -e s/$PREVIOUS.core.iterators.aggregation.conf.AggregatorConfiguration/org.apache.accumulo.core.iterators.aggregation.conf.AggregatorConfiguration/g \
 -e s/$PREVIOUS.core.iterators.aggregation.conf.AggregatorSet/org.apache.accumulo.core.iterators.aggregation.conf.AggregatorSet/g \
 -e s/$PREVIOUS.core.iterators.aggregation.LongSummation/org.apache.accumulo.core.iterators.aggregation.LongSummation/g \
 -e s/$PREVIOUS.core.iterators.aggregation.NumArraySummation/org.apache.accumulo.core.iterators.aggregation.NumArraySummation/g \
 -e s/$PREVIOUS.core.iterators.aggregation.NumSummation/org.apache.accumulo.core.iterators.aggregation.NumSummation/g \
 -e s/$PREVIOUS.core.iterators.aggregation.StringMax/org.apache.accumulo.core.iterators.aggregation.StringMax/g \
 -e s/$PREVIOUS.core.iterators.aggregation.StringMin/org.apache.accumulo.core.iterators.aggregation.StringMin/g \
 -e s/$PREVIOUS.core.iterators.aggregation.StringSummation/org.apache.accumulo.core.iterators.aggregation.StringSummation/g \
 -e s/$PREVIOUS.core.iterators.ColumnFamilyCounter/org.apache.accumulo.core.iterators.ColumnFamilyCounter/g \
 -e s/$PREVIOUS.core.iterators.ColumnFamilySkippingIterator/org.apache.accumulo.core.iterators.ColumnFamilySkippingIterator/g \
 -e s/$PREVIOUS.core.iterators.conf.ColumnToClassMapping/org.apache.accumulo.core.iterators.conf.ColumnToClassMapping/g \
 -e s/$PREVIOUS.core.iterators.conf.PerColumnIteratorConfig/org.apache.accumulo.core.iterators.conf.PerColumnIteratorConfig/g \
 -e s/$PREVIOUS.core.iterators.CountingIterator/org.apache.accumulo.core.iterators.CountingIterator/g \
 -e s/$PREVIOUS.core.iterators.DebugIterator/org.apache.accumulo.core.iterators.DebugIterator/g \
 -e s/$PREVIOUS.core.iterators.DefaultIteratorEnvironment/org.apache.accumulo.core.iterators.DefaultIteratorEnvironment/g \
 -e s/$PREVIOUS.core.iterators.DeletingIterator/org.apache.accumulo.core.iterators.DeletingIterator/g \
 -e s/$PREVIOUS.core.iterators.DevNull/org.apache.accumulo.core.iterators.DevNull/g \
 -e s/$PREVIOUS.core.iterators.FamilyIntersectingIterator/org.apache.accumulo.core.iterators.FamilyIntersectingIterator/g \
 -e s/$PREVIOUS.core.iterators.filter.AgeOffFilter/org.apache.accumulo.core.iterators.filter.AgeOffFilter/g \
 -e s/$PREVIOUS.core.iterators.filter.ColumnAgeOffFilter/org.apache.accumulo.core.iterators.filter.ColumnAgeOffFilter/g \
 -e s/$PREVIOUS.core.iterators.filter.ColumnQualifierFilter/org.apache.accumulo.core.iterators.filter.ColumnQualifierFilter/g \
 -e s/$PREVIOUS.core.iterators.filter.DeleteFilter/org.apache.accumulo.core.iterators.filter.DeleteFilter/g \
 -e s/$PREVIOUS.core.iterators.filter.Filter/org.apache.accumulo.core.iterators.filter.Filter/g \
 -e s/$PREVIOUS.core.iterators.filter.NoLabelFilter/org.apache.accumulo.core.iterators.filter.NoLabelFilter/g \
 -e s/$PREVIOUS.core.iterators.filter.RegExFilter/org.apache.accumulo.core.iterators.filter.RegExFilter/g \
 -e s/$PREVIOUS.core.iterators.filter.VisibilityFilter/org.apache.accumulo.core.iterators.filter.VisibilityFilter/g \
 -e s/$PREVIOUS.core.iterators.FilteringIterator/org.apache.accumulo.core.iterators.FilteringIterator/g \
 -e s/$PREVIOUS.core.iterators.FirstEntryInRowIterator/org.apache.accumulo.core.iterators.FirstEntryInRowIterator/g \
 -e s/$PREVIOUS.core.iterators.GrepIterator/org.apache.accumulo.core.iterators.GrepIterator/g \
 -e s/$PREVIOUS.core.iterators.HeapIterator/org.apache.accumulo.core.iterators.HeapIterator/g \
 -e s/$PREVIOUS.core.iterators.InterruptibleIterator/org.apache.accumulo.core.iterators.InterruptibleIterator/g \
 -e s/$PREVIOUS.core.iterators.IntersectingIterator/org.apache.accumulo.core.iterators.IntersectingIterator/g \
 -e s/$PREVIOUS.core.iterators.IterationInterruptedException/org.apache.accumulo.core.iterators.IterationInterruptedException/g \
 -e s/$PREVIOUS.core.iterators.IteratorEnvironment/org.apache.accumulo.core.iterators.IteratorEnvironment/g \
 -e s/$PREVIOUS.core.iterators.IteratorUtil/org.apache.accumulo.core.iterators.IteratorUtil/g \
 -e s/$PREVIOUS.core.iterators.LargeRowFilter/org.apache.accumulo.core.iterators.LargeRowFilter/g \
 -e s/$PREVIOUS.core.iterators.MultiIterator/org.apache.accumulo.core.iterators.MultiIterator/g \
 -e s/$PREVIOUS.core.iterators.NoLabelIterator/org.apache.accumulo.core.iterators.NoLabelIterator/g \
 -e s/$PREVIOUS.core.iterators.OptionDescriber/org.apache.accumulo.core.iterators.OptionDescriber/g \
 -e s/$PREVIOUS.core.iterators.OrIterator/org.apache.accumulo.core.iterators.OrIterator/g \
 -e s/$PREVIOUS.core.iterators.RegExIterator/org.apache.accumulo.core.iterators.RegExIterator/g \
 -e s/$PREVIOUS.core.iterators.RowDeletingIterator/org.apache.accumulo.core.iterators.RowDeletingIterator/g \
 -e s/$PREVIOUS.core.iterators.ScanCache/org.apache.accumulo.core.iterators.ScanCache/g \
 -e s/$PREVIOUS.core.iterators.SequenceFileIterator/org.apache.accumulo.core.iterators.SequenceFileIterator/g \
 -e s/$PREVIOUS.core.iterators.SkippingIterator/org.apache.accumulo.core.iterators.SkippingIterator/g \
 -e s/$PREVIOUS.core.iterators.SortedKeyIterator/org.apache.accumulo.core.iterators.SortedKeyIterator/g \
 -e s/$PREVIOUS.core.iterators.SortedKeyValueIterator/org.apache.accumulo.core.iterators.SortedKeyValueIterator/g \
 -e s/$PREVIOUS.core.iterators.SortedMapIterator/org.apache.accumulo.core.iterators.SortedMapIterator/g \
 -e s/$PREVIOUS.core.iterators.SourceSwitchingIterator/org.apache.accumulo.core.iterators.SourceSwitchingIterator/g \
 -e s/$PREVIOUS.core.iterators.SystemScanIterator/org.apache.accumulo.core.iterators.SystemScanIterator/g \
 -e s/$PREVIOUS.core.iterators.VersioningIterator/org.apache.accumulo.core.iterators.VersioningIterator/g \
 -e s/$PREVIOUS.core.iterators.WholeRowIterator/org.apache.accumulo.core.iterators.WholeRowIterator/g \
 -e s/$PREVIOUS.core.iterators.WrappingIterator/org.apache.accumulo.core.iterators.WrappingIterator/g \
 -e s/$PREVIOUS.core.iterators.AggregatingIteratorTest/org.apache.accumulo.core.iterators.AggregatingIteratorTest/g \
 -e s/$PREVIOUS.core.iterators.aggregation.conf.AggregatorConfigurationTest/org.apache.accumulo.core.iterators.aggregation.conf.AggregatorConfigurationTest/g \
 -e s/$PREVIOUS.core.iterators.aggregation.NumSummationTest/org.apache.accumulo.core.iterators.aggregation.NumSummationTest/g \
 -e s/$PREVIOUS.core.iterators.ColumnFamilySkippingIteratorTest/org.apache.accumulo.core.iterators.ColumnFamilySkippingIteratorTest/g \
 -e s/$PREVIOUS.core.iterators.DeletingIteratorTest/org.apache.accumulo.core.iterators.DeletingIteratorTest/g \
 -e s/$PREVIOUS.core.iterators.FamilyIntersectingIteratorTest/org.apache.accumulo.core.iterators.FamilyIntersectingIteratorTest/g \
 -e s/$PREVIOUS.core.iterators.filter.ColumnFilterTest/org.apache.accumulo.core.iterators.filter.ColumnFilterTest/g \
 -e s/$PREVIOUS.core.iterators.FilteringIteratorTest/org.apache.accumulo.core.iterators.FilteringIteratorTest/g \
 -e s/$PREVIOUS.core.iterators.IntersectingIteratorTest/org.apache.accumulo.core.iterators.IntersectingIteratorTest/g \
 -e s/$PREVIOUS.core.iterators.IterUtilTest/org.apache.accumulo.core.iterators.IterUtilTest/g \
 -e s/$PREVIOUS.core.iterators.LargeRowFilterTest/org.apache.accumulo.core.iterators.LargeRowFilterTest/g \
 -e s/$PREVIOUS.core.iterators.MultiIteratorTest/org.apache.accumulo.core.iterators.MultiIteratorTest/g \
 -e s/$PREVIOUS.core.iterators.RegExIteratorTest/org.apache.accumulo.core.iterators.RegExIteratorTest/g \
 -e s/$PREVIOUS.core.iterators.RowDeletingIteratorTest/org.apache.accumulo.core.iterators.RowDeletingIteratorTest/g \
 -e s/$PREVIOUS.core.iterators.SourceSwitchingIteratorTest/org.apache.accumulo.core.iterators.SourceSwitchingIteratorTest/g \
 -e s/$PREVIOUS.core.iterators.VersioningIteratorTest/org.apache.accumulo.core.iterators.VersioningIteratorTest/g \
 -e s/$PREVIOUS.core.iterators.WholeRowIteratorTest/org.apache.accumulo.core.iterators.WholeRowIteratorTest/g \
 -e s/$PREVIOUS.server.master.balancer.DefaultLoadBalancer/org.apache.accumulo.server.master.balancer.DefaultLoadBalancer/g \
 -e s/$PREVIOUS.server.master.balancer.LoggerBalancer/org.apache.accumulo.server.master.balancer.LoggerBalancer/g \
 -e s/$PREVIOUS.server.master.balancer.LoggerUser/org.apache.accumulo.server.master.balancer.LoggerUser/g \
 -e s/$PREVIOUS.server.master.balancer.SimpleLoggerBalancer/org.apache.accumulo.server.master.balancer.SimpleLoggerBalancer/g \
 -e s/$PREVIOUS.server.master.balancer.TableLoadBalancer/org.apache.accumulo.server.master.balancer.TableLoadBalancer/g \
 -e s/$PREVIOUS.server.master.balancer.TabletBalancer/org.apache.accumulo.server.master.balancer.TabletBalancer/g \
 -e s/$PREVIOUS.server.master.balancer.TServerUsesLoggers/org.apache.accumulo.server.master.balancer.TServerUsesLoggers/g \
 -e s/$PREVIOUS.server.master.balancer.DefaultLoadBalancerTest/org.apache.accumulo.server.master.balancer.DefaultLoadBalancerTest/g \
 -e s/$PREVIOUS.server.master.balancer.SimpleLoggerBalancerTest/org.apache.accumulo.server.master.balancer.SimpleLoggerBalancerTest/g \
 -e s/$PREVIOUS.core.constraints.Constraint/org.apache.accumulo.core.constraints.Constraint/g \
 -e s/$PREVIOUS.core.constraints.Violations/org.apache.accumulo.core.constraints.Violations/g \
 -e s/$PREVIOUS.examples.constraints.AlphaNumKeyConstraint/org.apache.accumulo.examples.constraints.AlphaNumKeyConstraint/g \
 -e s/$PREVIOUS.examples.constraints.NumericValueConstraint/org.apache.accumulo.examples.constraints.NumericValueConstraint/g \
 -e s/$PREVIOUS.server.constraints.ConstraintChecker/org.apache.accumulo.server.constraints.ConstraintChecker/g \
 -e s/$PREVIOUS.server.constraints.ConstraintLoader/org.apache.accumulo.server.constraints.ConstraintLoader/g \
 -e s/$PREVIOUS.server.constraints.MetadataConstraints/org.apache.accumulo.server.constraints.MetadataConstraints/g \
 -e s/$PREVIOUS.server.constraints.SystemConstraint/org.apache.accumulo.server.constraints.SystemConstraint/g \
 -e s/$PREVIOUS.server.constraints.UnsatisfiableConstraint/org.apache.accumulo.server.constraints.UnsatisfiableConstraint/g \
 -e s/$PREVIOUS.server.constraints.MetadataConstraintsTest/org.apache.accumulo.server.constraints.MetadataConstraintsTest/g \
 -e "s%/$PREVIOUS/$INSTANCE%/accumulo/$INSTANCE%g" \
 "$TEMP"
verbose Loading zookeeper with renamed values
"$ACCUMULO_HOME"/bin/accumulo org.apache.accumulo.server.util.RestoreZookeeper "$ZOOKEEPER" "$TEMP" --overwrite || fail unable to load new settings into zookeeper
verbose Making final instance name setting

"$ZOOKEEPER_COMMAND" -server "$ZOOKEEPER" 'create /accumulo/instances instances' >/dev/null 2>/dev/null

"$ZOOKEEPER_COMMAND" -server "$ZOOKEEPER" "delete /accumulo/instances/$NAME" >/dev/null 2>/dev/null

"$ZOOKEEPER_COMMAND" -server "$ZOOKEEPER" "create /accumulo/instances/$NAME $INSTANCE" >/dev/null || fail unable to set the instance name

verbose Renaming the main directory in HDFS
"$HADOOP_COMMAND" fs -mv "$DFSBEFORE" "$DFSAFTER" || fail unable to move $PREVIOUS to the new location
verbose Rename successful
