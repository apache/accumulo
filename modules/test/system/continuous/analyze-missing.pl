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

if(scalar(@ARGV) != 4){
	print "Usage : analyze-missing.pl <accumulo home> <continuous log dir> <user> <pass> \n";
	exit(1);
}

$ACCUMULO_HOME=$ARGV[0];
$CONTINUOUS_LOG_DIR=$ARGV[1];
$USER=$ARGV[2];
$PASS=$ARGV[3];


@missing = `grep MIS $CONTINUOUS_LOG_DIR/*.err`;



for $miss (@missing) {
	chomp($miss);
	($file, $type, $time, $row) = split(/[: ]/, $miss);

	substr($file, -3, 3, "out");

	$prevRowLine = `grep -B 1 $row $file | grep SRQ | grep -v $row`;

	@prla = split(/\s+/, $prevRowLine);
	$prevRow = $prla[2];
#	print $prevRow."\n";

	$aScript = `mktemp /tmp/miss_script.XXXXXXXXXX`;
	chomp($aScript);
	open(AS, ">$aScript") || die;

	print AS "table ci\n";
	print AS "scan -b $prevRow -e $prevRow\n";
	print AS "scan -b $row -e $row\n";
	print AS "quit\n";
	close(AS);

	$exist = 0;
	$ingestIDSame = 0;
	$ingestId = "";
	$count = 0;

	@entries = `$ACCUMULO_HOME/bin/accumulo shell -u $USER -p $PASS -f $aScript | grep $row`;
	system("rm $aScript");

	for $entry (@entries){
		chomp($entry);
		@entryA = split(/[: ]+/, $entry);
		if($entryA[0] eq $row){
			$exist = 1;

			if($entryA[4] eq $ingestId){
				$ingestIDSame = 1;
			}
		}else{
			$ingestId = $entryA[4];
			$count = hex($entryA[5]);
		}
	}


	#look in ingest logs
	@ingestLogs = `ls  $CONTINUOUS_LOG_DIR/*ingest*.out`;
	@flushTimes = ();
	chomp(@ingestLogs);
	for $ingestLog (@ingestLogs){
		open(IL, "<$ingestLog") || die;
		

		while($firstLine = <IL>){
			chomp($firstLine);
			if($firstLine =~ /UUID.*/){
				last;
			}
		}

		@iinfo = split(/\s+/,$firstLine);
		if($iinfo[2] eq $ingestId){
			while($line = <IL>){
				if($line =~ /FLUSH (\d+) \d+ \d+ (\d+) \d+/){
					push(@flushTimes, $1);
					if(scalar(@flushTimes) > 3){
						shift(@flushTimes);
					}
					if($count < $2){
						last;
					}
				}
			}
		}
		
		

		close(IL);
	
		if(scalar(@flushTimes) > 0){
			last;
		}
	} 

	$its0 = strftime "%m/%d/%Y_%H:%M:%S", gmtime($flushTimes[0]/1000);
	$its1 = strftime "%m/%d/%Y_%H:%M:%S", gmtime($flushTimes[1]/1000);
	$mts = strftime "%m/%d/%Y_%H:%M:%S", gmtime($time/1000);

	print "$row $exist $ingestIDSame $prevRow $ingestId   $its0   $its1   $mts\n";
}

