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

if(scalar(@ARGV) == 2 && $ARGV[0] eq "-bt"){
	$BIN_TIME=$ARGV[1];
}elsif(scalar(@ARGV) == 0){
	$BIN_TIME=900;
}else{
	print "Usage : report.pl [-bt <bin time>]\n";
	exit;
}


$LOG_DIR = "logs";
$ACCUMULO_HOME="../../..";
$REPORT_DIR = strftime "report_%Y%m%d%H%M", localtime;

mkdir("$REPORT_DIR");

open (HTML, ">$REPORT_DIR/report.html");

print HTML "<html><body>\n";

$misCount = `grep MIS $LOG_DIR/*_walk.err | wc -l`;

if($misCount > 0){
	print HTML "<HR width=50% size=4>\n";
	print HTML "<center><P><B color=red>WARNING : The walkers saw missing nodes, this should not happen</B><P></center>\n";
	print HTML "<HR width=50% size=4>\n";
}

plot("cat $LOG_DIR/*_stats.out", $BIN_TIME, 0, 2, "AVG", "entries", "Entries over time");
plot("cat $LOG_DIR/*_stats.out", $BIN_TIME, 0, 3, "AMM", "ingest_rate", "Ingest rate over time");
plot("egrep 'SRQ|FSR' $LOG_DIR/*_walk.out", $BIN_TIME, 1, 3, "AMM", "query_latency", "Row lookup latency (in milliseconds) over time");
plot("egrep 'SRQ|FSR' $LOG_DIR/*_walk.out", 3600, 1, 3, "COUNT", "query_count", "# rows looked up in each hour");
plot("grep 'BRQ' $LOG_DIR/*_batch_walk.out", $BIN_TIME, 1, 5, "AMM", "batch_walk_rate", "batch walkers average lookup rate" );
plot("cat $LOG_DIR/*_stats.out", $BIN_TIME, 0, 10, "AVG", "tablets", "Table tablets online over time");
plot("cat $LOG_DIR/*_stats.out", $BIN_TIME, 0, 25, "AMM_HACK1", "files_per_tablet", "Files per tablet");
plot("cat $LOG_DIR/*_stats.out", $BIN_TIME, 0, 1, "AVG", "tservers", "Tablet servers over time");
plot("cat $LOG_DIR/*_stats.out", $BIN_TIME, 0, 11, "AVG", "du", "HDFS usage over time");
plot("cat $LOG_DIR/*_stats.out", $BIN_TIME, 0, 12, "AVG", "dirs", "HDFS # dirs over time");
plot("cat $LOG_DIR/*_stats.out", $BIN_TIME, 0, 13, "AVG", "files", "HDFS # files over time");
plot("cat $LOG_DIR/*_stats.out", $BIN_TIME, 0, 17, "AVG", "maps", "# map task over time");
plot("cat $LOG_DIR/*_stats.out", $BIN_TIME, 0, 19, "AVG", "reduces", "# reduce task over time");

print HTML "<P><h2>Config</h2>\n";
print HTML "<UL>\n";
for $config_file (glob("$LOG_DIR/*_config.out")){
	@path = split(/\//,$config_file);
        $file_name = $path[$path - 1];
	system("cp $config_file $REPORT_DIR/$file_name");
	print HTML "<li><a href='$file_name'>$file_name</a>\n";
}
print HTML "</UL>\n";


print HTML "<P><h2>Lookup times histogram</h2>\n";
print HTML "<pre>\n";
print HTML `cat $LOG_DIR/*_walk.out | $ACCUMULO_HOME/bin/accumulo org.apache.accumulo.server.test.continuous.PrintScanTimeHistogram`;
print HTML "</pre>\n";

print HTML "</body></html>\n";
close(HTML);

sub plot {
	my $cmd = shift(@_);
	my $period = shift(@_);
	my $time_col = shift(@_);
	my $data_col = shift(@_);
	my $op = shift(@_);
	my $output = shift(@_);
	my $title = shift(@_);

	system("$cmd | $ACCUMULO_HOME/bin/accumulo org.apache.accumulo.server.test.continuous.TimeBinner $period $time_col $data_col $op MM/dd/yy-HH:mm:ss > $REPORT_DIR/$output.dat");
	gnuplot("$REPORT_DIR/$output.dat", "$REPORT_DIR/$output.png", $op eq "AMM" || $op eq "AMM_HACK1");

	print HTML "<P><h2>$title</h2><img src='$output.png'>\n";
}

sub gnuplot {
	my $input = shift(@_);
	my $output = shift(@_);
	my $yerr = shift(@_);

	open(GNUP, "|gnuplot > $output");	

	print GNUP "set xdata time\n";
	print GNUP "set timefmt \"%m/%d/%y-%H:%M:%S\"\n";
	print GNUP "set format x \"%m/%d\"\n";
	print GNUP "set offsets 1,1,1,1\n";
	print GNUP "set size 1.25,1.25\n";
	print GNUP "set terminal png\n";
	if($yerr){
		print GNUP "plot \"$input\" using 1:2:3:4 with yerrorlines\n";
	}else{
		print GNUP "plot \"$input\" using 1:2\n";
	}

	close(GNUP);
}
	


