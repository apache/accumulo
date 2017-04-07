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

use strict;
use warnings;
use POSIX qw(strftime);
use Getopt::Long;
use Pod::Usage;

my $help = 0;
my $man = 0;
my $sleep = 10;
my $superuser = 'hdfs';
my $hdfsCmd;
if( defined $ENV{'HADOOP_PREFIX'} ){
  $hdfsCmd = $ENV{'HADOOP_PREFIX'} . '/share/hadoop/hdfs/bin/hdfs';
}
my $sudo;
my $nameservice;

GetOptions('help|?' => \$help, 'man' => \$man, 'sleep=i' => \$sleep, 'nameservice=s' => \$nameservice, 'superuser=s' => \$superuser, 'hdfs-cmd=s' => \$hdfsCmd, 'sudo:s' => \$sudo) or pod2usage(2);
pod2usage(-exitval => 0, -verbose => 1) if $help;
pod2usage(-exitval => 0, -verbose => 2) if $man;
pod2usage(-exitval => 1, -verbose => 1, -message => '$HADOOP_PREFIX not defined and no hdfs-cmd given. please use --hdfs-cmd to specify where your hdfs cli is.') if not defined $hdfsCmd;
pod2usage(-exitval => 1, -verbose => 1, -message => "Your specified hdfs cli '$hdfsCmd' is not executable.") if not -x $hdfsCmd;
if( defined $sudo and "" eq $sudo ){
  $sudo = `which sudo`;
  pod2usage(-exitval => 1, -verbose => 1, -message => "Error attempting to find the sudo command, please specify it with --sudo /path/to/sudo") if 0 != $?;
  chomp($sudo);
}
if( defined $sudo ){
  pod2usage(-exitval => 1, -verbose => 1, -message => "Your specified sudo command '$sudo' is not executable.") if not -x $sudo;
}

my $needsudo = defined $sudo;
my $haadmin = "$hdfsCmd haadmin";
if($needsudo) {
  $haadmin = "$sudo -u $superuser $haadmin";
  print STDERR "Starting HDFS agitator, configured to fail over every $sleep minutes. will run hdfs command '$hdfsCmd' as user '$superuser' via '$sudo'.\n";
} else {
  print STDERR "Starting HDFS agitator, configured to fail over every $sleep minutes. will run hdfs command '$hdfsCmd' as the current user.\n";
}
while(1){
  sleep($sleep * 60);
  my $t = strftime "%Y%m%d %H:%M:%S", localtime;
  my @failServices;
  if( defined $nameservice ){
    @failServices = ($nameservice);
  } else {
    my $nameservicesRaw = `$hdfsCmd getconf -confKey dfs.nameservices`;
    if(0 != $?) {
      print STDERR "$t HDFS CLI failed. please see --help to set it correctly\n";
      exit(1);
    }
    chomp($nameservicesRaw);
    my @nameservices = split(/,/, $nameservicesRaw);
    if(1 > scalar(@nameservices)) {
      print STDERR "$t No HDFS NameServices found. Are you sure you're running in HA?\n";
      exit(1);
    }
    if(rand(1) < .5){
      my $serviceToFail = $nameservices[int(rand(scalar(@nameservices)))];
      print STDERR "$t Failing over nameservice $serviceToFail\n";
      @failServices = ($serviceToFail);
    } else {
      print STDERR "$t Failing over all nameservices\n";
      @failServices = @nameservices;
    }
  }
  for my $toFail (@failServices){
    my $namenodesRaw = `$hdfsCmd getconf -confKey dfs.ha.namenodes.$toFail`;
    if(0 != $?) {
      print STDERR "$t HDFS CLI failed to look up namenodes in service $toFail.\n";
      exit(1);
    }
    chomp($namenodesRaw);
    my @namenodes = split(/,/, $namenodesRaw);
    if(2 > scalar(@namenodes)) {
      print STDERR "$t WARN NameService $toFail does not have at least 2 namenodes according to the HDFS configuration, skipping.\n";
      next;
    }
    my $active;
    for my $namenode (@namenodes){
      my $status = `$haadmin -ns $toFail -getServiceState $namenode`;
      if(0 != $?) {
        if($needsudo) {
          print STDERR "$t WARN Error while attempting to get the service state of $toFail :: $namenode\n";
          $status = 'error';
        } else {
          print STDERR "$t WARN Current user may not run the HDFS haadmin utility, attempting to sudo to the $superuser user.\n";
          $needsudo = 1;
          if(not defined $sudo) {
            $sudo = `which sudo`;
            pod2usage(-exitval => 1, -verbose => 1, -message => "Error attempting to find the sudo command, please specify it with --sudo") if 0 != $?;
            chomp($sudo);
            pod2usage(-exitval => 1, -verbose => 1, -message => "The sudo command '$sudo' is not executable. please specify sudo with --sudo") if not -x $sudo;
          }
          $haadmin = "$sudo -u $superuser $haadmin";
          redo;
        }
      }
      chomp($status);
      if( 'active' eq $status ){
        $active = $namenode;
        last;
      }
    }
    if( defined $active ){
      my @standby = grep { $_ ne $active } @namenodes;
      my $newActive = $standby[int(rand(scalar(@standby)))];
      print STDERR "$t Transitioning nameservice $toFail from $active to $newActive\n";
      my $cmd = "$haadmin -ns $toFail -failover $active $newActive";
      print "$t $cmd\n";
      system($cmd);
    } else {
      my $newActive = $namenodes[int(rand(scalar(@namenodes)))];
      print STDERR "$t WARN nameservice $toFail did not have an active namenode. Transitioning a random namenode to active. This will fail if HDFS is configured for automatic failover.\n";
      my $cmd = "$haadmin -ns $toFail -transitionToActive $newActive";
      print "$t $cmd\n";
      system($cmd);
    }
  }
}
__END__

=head1 NAME

hdfs-agitator - causes HDFS to failover

=head1 DESCRIPTION

Sleeps for a configurable amount of time, then causes a NameNode failover in one
or more HDFS NameServices. If a given NameService does not have an Active
NameNode when it comes time to failover, a random standby is promoted.

Only works on HDFS versions that support HA configurations and the haadmin
command. In order to function, the user running this script must be able to
use the haadmin command. This requires access to an HDFS superuser. By default,
it will attempt to sudo to perform calls.

=head1 SYNOPSIS

hdfs-agitator [options]

  Options:
    --help         Brief help message
    --man          Full documentation
    --sleep        Time to sleep between failovers in minutes. Default 10
    --superuser    HDFS superuser. Default 'hdfs'
    --hdfs-cmd     hdfs command path. Default '$HADOOP_PREFIX/share/hadoop/hdfs/bin/hdfs'
    --nameservice  Limit failovers to specified nameservice. Default all nameservices
    --sudo         command to call to sudo to the HDFS superuser. Default 'sudo' if needed.

=head1 OPTIONS

=over 8

=item B<--sleep>

Sleep the given number of minutes between attempts to fail over nameservices.

=item B<--nameservice>

Limit failover attempts to the given nameservice. By default, we attempt ot list
all known nameservices and choose either one or all of them to failover in a
given cycle.

=item B<--superuser>

An HDFS superuser capable of running the haadmin command. Defaults to "hdfs".

=item B<--hdfs-cmd>

Path to the HDFS cli. Will be used both for non-administrative commands (e.g.
listing the nameservices and serviceids in a given nameservice) and admin-only
actions such as checking status and failing over.

Defaults to using $HADOOP_PREFIX.

=item B<--sudo>

Command to allow us to act as the given HDFS superuser. By default we assume the current user
can run HDFS administrative commands. When this argument is specified we will instead attempt
to use the HDFS superuser instead. If given an argument, it will be called like
sudo, i.e. "sudo -u $superuser $cmd". Defaults to "sudo" on the shell's path.

=back

=head1 SEE ALSO

See the Apache Hadoop documentation on configuring HDFS HA

=over 8

=item B<HA with QJM>

http://hadoop.apache.org/docs/r2.2.0/hadoop-yarn/hadoop-yarn-site/HDFSHighAvailabilityWithQJM.html#Administrative_commands

=item B<HA with NFS>

http://hadoop.apache.org/docs/r2.2.0/hadoop-yarn/hadoop-yarn-site/HDFSHighAvailabilityWithNFS.html#Administrative_commands

=back
