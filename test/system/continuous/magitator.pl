#! /usr/bin/env perl

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


use POSIX qw(strftime);

if(scalar(@ARGV) != 2){
	print "Usage : magitator.pl <sleep before kill in minutes> <sleep before start-all in minutes>\n";
	exit(1);
}


$ACCUMULO_HOME="../../..";

$sleep1 = $ARGV[0];
$sleep2 = $ARGV[1];

@mastersRaw = `cat $ACCUMULO_CONF_DIR/masters`;
chomp(@mastersRaw);

for $master (@mastersRaw){
	if($master eq "" || substr($master,0,1) eq "#"){
		next;
	}

	push(@masters, $master);
}


while(1){
	sleep($sleep1 * 60);
	$t = strftime "%Y%m%d %H:%M:%S", localtime;
	if(rand(1) < .5){
		$masterNodeToWack = $masters[int(rand(scalar(@masters)))];
		print STDERR "$t Killing master on $masterNodeToWack\n";
		$cmd = "ssh $masterNodeToWack \"pkill -f '[ ]org.apache.accumulo.start.*master'\"";
		print "$t $cmd\n";
		system($cmd);
	}else{
		print STDERR "$t Killing all masters\n";
		$cmd = "pssh -h $ACCUMULO_CONF_DIR/masters \"pkill -f '[ ]org.apache.accumulo.start.*master'\" < /dev/null";
		print "$t $cmd\n";
		system($cmd);
		$cmd = "pssh -h $ACCUMULO_CONF_DIR/masters \"pkill -f '[ ]org.apache.accumulo.start.*gc'\" < /dev/null";
		print "$t $cmd\n";
		system($cmd);
	}

	sleep($sleep2 * 60);
	$t = strftime "%Y%m%d %H:%M:%S", localtime;
	print STDERR "$t Running start-all\n";

	$cmd = "$ACCUMULO_HOME/bin/start-all.sh --notSlaves";
	print "$t $cmd\n";
	system($cmd);
}


