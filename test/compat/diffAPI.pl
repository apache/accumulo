#!/usr/bin/perl

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


if(scalar(@ARGV) != 2){
	print "Usage : diffAPI.pl <core jar 1> <core jar 2>\n";
	exit(-1);
}

$jar1 = $ARGV[0];
$jar2 = $ARGV[1];

$gtCmd = 'egrep "accumulo/core/client/.*class|accumulo/core/data/.*class" | grep -v accumulo/core/client/impl | grep -v  accumulo/core/data/thrift | tr / .';

@classes1 = `jar tf $jar1 | $gtCmd`;
@classes2 = `jar tf $jar2 | $gtCmd`;

mkdir("diffWorkDir");
mkdir("diffWorkDir/jar1");
mkdir("diffWorkDir/jar2");

for $class (@classes1){
	$class = substr($class, 0, length($class) - 7);
	system("javap -classpath $jar1 $class | sort > diffWorkDir/jar1/$class");
}

for $class (@classes2){
	$class = substr($class, 0, length($class) - 7);
	system("javap -classpath $jar2 $class | sort > diffWorkDir/jar2/$class");
}

system("diff -u diffWorkDir/jar1 diffWorkDir/jar2");
system("rm -rf diffWorkDir");

