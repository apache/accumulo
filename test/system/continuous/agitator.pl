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

if(scalar(@ARGV) != 4 && scalar(@ARGV) != 2){
	print "Usage : agitator.pl <sleep before kill in minutes> <sleep before tup in minutes> [<min kill> <max kill>]\n";
	exit(1);
}

$ACCUMULO_HOME="../../..";

$sleep1 = $ARGV[0];
$sleep2 = $ARGV[1];

if(scalar(@ARGV) == 4){
	$minKill = $ARGV[2];
	$maxKill = $ARGV[3];
}else{
	$minKill = 1;
	$maxKill = 1;
}

if($minKill > $maxKill){
	die("minKill > maxKill $minKill > $maxKill");
}

@slavesRaw = `cat $ACCUMULO_HOME/conf/slaves`;
chomp(@slavesRaw);

for $slave (@slavesRaw){
	if($slave eq "" || substr($slave,0,1) eq "#"){
		next;
	}

	push(@slaves, $slave);
}


if(scalar(@slaves) < $maxKill){
	print STDERR "WARN setting maxKill to ".scalar(@slaves)."\n";
	$maxKill = scalar(@slaves);
}

while(1){

	$numToKill = int(rand($maxKill - $minKill + 1)) + $minKill;
	%killed = {};

	for($i = 0; $i < $numToKill; $i++){
		$server = "";	
		while($server eq "" || $killed{$server} != undef){
			$index = int(rand(scalar(@slaves)));
			$server = $slaves[$index];
		}

		$killed{$server} = 1;

		$t = strftime "%Y%m%d %H:%M:%S", localtime;
	
		$rn = rand(1);
		print STDERR "$t Killing $server\n";
		system("$ACCUMULO_HOME/bin/stop-server.sh $server \"accumulo-start.*.jar\" tserver KILL");
	}

	sleep($sleep2 * 60);
	$t = strftime "%Y%m%d %H:%M:%S", localtime;
	print STDERR "$t Running tup\n";
	system("$ACCUMULO_HOME/bin/tup.sh");

	sleep($sleep1 * 60);
}

