#!/usr/bin/perl

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

