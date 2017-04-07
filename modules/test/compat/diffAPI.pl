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

sub trim($)
{
	my $string = shift;
	$string =~ s/^\s+//;
	$string =~ s/\s+$//;
	return $string;
}

sub getDeprecated {
        my($jar, $class) = @_;
        
	open(JAVAP, "javap -verbose -public -classpath '$jar' '$class'|");

        my $lastMethod = "";
        my %deprecated;

        while(<JAVAP>){
		chomp();
                if(/^public\s/){
                        $lastMethod = $_;
                }
                if(/Deprecated\:\strue/){
			$lastMethod =~ s/\s+/ /g;
                        $deprecated{$lastMethod}="true";
                }
        }

        close(JAVAP);

        return %deprecated;
}

sub annotateDeprecated {
        my($jar, $class, $deprecated, $outFile) = @_;
	open(JAVAP, "javap -public -classpath '$jar' '$class'|");
	open(OUT, ">$outFile");
	my @javapOut =  <JAVAP>;
	@javapOut = sort(@javapOut);

	for my $line (@javapOut){
		my $trimLine = trim($line);
		chomp($line);
		$trimLine =~ s/\s+/ /g;
		if($deprecated->{$trimLine}){
			print OUT "$line DEPRECATED\n";
		}else{
			print OUT "$line\n";
		}
	}

	close(JAVAP);
	close(OUT);
	
}

if(scalar(@ARGV) != 2){
	print "Usage : diffAPI.pl <core jar 1> <core jar 2>\n";
	exit(-1);
}

$jar1 = $ARGV[0];
$jar2 = $ARGV[1];

$gtCmd = 'egrep "accumulo/core/client/.*class|accumulo/core/data/.*class" | grep -v accumulo/core/client/impl | grep -v  accumulo/core/data/thrift | egrep -v "Impl.*class$" | tr / .';

@classes1 = `jar tf $jar1 | $gtCmd`;
@classes2 = `jar tf $jar2 | $gtCmd`;

mkdir("diffWorkDir");
mkdir("diffWorkDir/jar1");
mkdir("diffWorkDir/jar2");

for $class (@classes1){
	$class = substr($class, 0, length($class) - 7);
	%deprecated = getDeprecated($jar1, $class);
	annotateDeprecated($jar1, $class, \%deprecated, "diffWorkDir/jar1/$class");
}

for $class (@classes2){
	$class = substr($class, 0, length($class) - 7);
	%deprecated = getDeprecated($jar2, $class);
	annotateDeprecated($jar2, $class, \%deprecated, "diffWorkDir/jar2/$class");
}

system("diff -u diffWorkDir/jar1 diffWorkDir/jar2");
system("rm -rf diffWorkDir");

