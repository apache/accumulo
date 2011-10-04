#! /usr/bin/env perl

use POSIX qw(strftime);


$LOG_DIR = "logs";
$ACCUMULO_HOME="../../..";
$REPORT_DIR = strftime "report_%Y%m%d%H%M", localtime;
$BIN_TIME = 900;

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
plot("cat $LOG_DIR/*_stats.out", $BIN_TIME, 0, 3, "AVG", "ingest_rate", "Ingest rate over time");
plot("egrep 'SRQ|FSR' $LOG_DIR/*_walk.out", $BIN_TIME, 1, 3, "AVG", "query_latency", "Row lookup latency (in milliseconds) over time");
plot("egrep 'SRQ|FSR' $LOG_DIR/*_walk.out", 3600, 1, 3, "COUNT", "query_count", "# rows looked up in each hour");
plot("grep 'BRQ' $LOG_DIR/*_batch_walk.out", $BIN_TIME, 1, 5, "AVG", "batch_walk_rate", "batch walkers average lookup rate" );
plot("cat $LOG_DIR/*_stats.out", $BIN_TIME, 0, 10, "AVG", "tablets", "Table tablets online over time");
plot("cat $LOG_DIR/*_stats.out", $BIN_TIME, 0, 25, "AVG", "files_per_tablet", "Files per tablet");
plot("cat $LOG_DIR/*_stats.out", $BIN_TIME, 0, 1, "AVG", "tservers", "Tablet servers over time");
plot("cat $LOG_DIR/*_stats.out", $BIN_TIME, 0, 11, "AVG", "du", "HDFS usage over time");
plot("cat $LOG_DIR/*_stats.out", $BIN_TIME, 0, 12, "AVG", "dirs", "HDFS # dirs over time");
plot("cat $LOG_DIR/*_stats.out", $BIN_TIME, 0, 13, "AVG", "files", "HDFS # files over time");
plot("cat $LOG_DIR/*_stats.out", $BIN_TIME, 0, 17, "AVG", "maps", "# map task over time");
plot("cat $LOG_DIR/*_stats.out", $BIN_TIME, 0, 19, "AVG", "reduces", "# reduce task over time");

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
	gnuplot("$REPORT_DIR/$output.dat", "$REPORT_DIR/$output.png");

	print HTML "<P><h2>$title</h2><img src='$output.png'>\n";
}

sub gnuplot {
	my $input = shift(@_);
	my $output = shift(@_);

	open(GNUP, "|gnuplot > $output");	

	print GNUP "set xdata time\n";
	print GNUP "set timefmt \"%m/%d/%y-%H:%M:%S\"\n";
	print GNUP "set format x \"%m/%d\"\n";
	print GNUP "set offsets 1,1,1,1\n";
	print GNUP "set size 1.25,1.25\n";
	print GNUP "set terminal png\n";
	print GNUP "plot \"$input\" using 1:2\n";

	close(GNUP);
}
	


