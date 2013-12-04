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
use Cwd qw();

if(scalar(@ARGV) != 6 && scalar(@ARGV) != 4){
  print "Usage : agitator.pl <min sleep before kill in minutes>[:max sleep before kill in minutes] <min sleep before tup in minutes>[:max sleep before tup in minutes] hdfs_user accumulo_user [<min kill> <max kill>]\n";
  exit(1);
}

$myself=`whoami`;
chomp($myself);
$am_root=($myself eq 'root');

$cwd=Cwd::cwd();
$ACCUMULO_HOME=$cwd . '/../../..';
$HADOOP_PREFIX=$ENV{"HADOOP_PREFIX"};

print "Current directory: $cwd\n";
print "ACCUMULO_HOME=$ACCUMULO_HOME\n";
print "HADOOP_PREFIX=$HADOOP_PREFIX\n";

@sleeprange1 = split(/:/, $ARGV[0]);
$sleep1 = $sleeprange1[0];

@sleeprange2 = split(/:/, $ARGV[1]);
$sleep2 = $sleeprange2[0];

if (scalar(@sleeprange1) > 1) {
  $sleep1max = $sleeprange1[1] + 1;
} else {
  $sleep1max = $sleep1;
}

if ($sleep1 > $sleep1max) {
  die("sleep1 > sleep1max $sleep1 > $sleep1max");
}

if (scalar(@sleeprange2) > 1) {
  $sleep2max = $sleeprange2[1] + 1;
} else {
  $sleep2max = $sleep2;
}

if($sleep2 > $sleep2max){
  die("sleep2 > sleep2max $sleep2 > $sleep2max");
}

if(defined $ENV{'ACCUMULO_CONF_DIR'}){
  $ACCUMULO_CONF_DIR = $ENV{'ACCUMULO_CONF_DIR'};
}else{
  $ACCUMULO_CONF_DIR = $ACCUMULO_HOME . '/conf';
}

$HDFS_USER=$ARGV[2];
$ACCUMULO_USER=$ARGV[3];

$am_hdfs_user=($HDFS_USER eq $myself);
$am_accumulo_user=($ACCUMULO_USER eq $myself);

if(scalar(@ARGV) == 6){
  $minKill = $ARGV[4];
  $maxKill = $ARGV[5];
}else{
  $minKill = 1;
  $maxKill = 1;
}

if($minKill > $maxKill){
  die("minKill > maxKill $minKill > $maxKill");
}

@slavesRaw = `cat $ACCUMULO_CONF_DIR/slaves`;
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

if ($minKill > $maxKill){
  print STDERR "WARN setting minKill to equal maxKill\n";
  $minKill = $maxKill;
}

while(1){

  $numToKill = int(rand($maxKill - $minKill + 1)) + $minKill;
  %killed = {};
  $server = "";
  $kill_tserver = 0;
  $kill_datanode = 0;

  for($i = 0; $i < $numToKill; $i++){
    while($server eq "" || $killed{$server} != undef){
      $index = int(rand(scalar(@slaves)));
      $server = $slaves[$index];
    }

    $killed{$server} = 1;

    $t = strftime "%Y%m%d %H:%M:%S", localtime;

    $rn = rand(1);
    if ($rn <.33) {
      $kill_tserver = 1;
      $kill_datanode = 1;
    } elsif ($rn < .66) {
      $kill_tserver = 1;
      $kill_datanode = 0;
    } else {
      $kill_tserver = 0;
      $kill_datanode = 1;
    }

    print STDERR "$t Killing $server $kill_tserver $kill_datanode\n";
    if ($kill_tserver) {
      if ($am_root) {
        # We're root, switch to the Accumulo user and try to stop gracefully
        system("su -c '$ACCUMULO_HOME/bin/stop-server.sh $server \"accumulo-start.jar\" tserver KILL' - $ACCUMULO_USER");
      } elsif ($am_accumulo_user) {
        # We're the accumulo user, just run the commandj
        system("$ACCUMULO_HOME/bin/stop-server.sh $server 'accumulo-start.jar' tserver KILL");
      } else {
        # We're not the accumulo user, try to use sudo
        system("sudo -u $ACCUMULO_USER $ACCUMULO_HOME/bin/stop-server.sh $server accumulo-start.jar tserver KILL");
      }
    }

    if ($kill_datanode) {
      if ($am_root) {
        # We're root, switch to HDFS to ssh and kill the process
        system("su -c 'ssh $server pkill -9 -f [p]roc_datanode' - $HDFS_USER");
      } elsif ($am_hdfs_user) {
        # We're the HDFS user, just kill the process
        system("ssh $server \"pkill -9 -f '[p]roc_datanode'\"");
      } else {
        # We're not the hdfs user, try to use sudo
        system("ssh $server 'sudo -u $HDFS_USER pkill -9 -f \'[p]roc_datanode\''");
      }
    }
  }

  $nextsleep2 = int(rand($sleep2max - $sleep2)) + $sleep2;
  sleep($nextsleep2 * 60);
  $t = strftime "%Y%m%d %H:%M:%S", localtime;
  print STDERR "$t Running tup\n";
  if ($am_root) {
    # Running as root, su to the accumulo user
    system("su -c $ACCUMULO_HOME/bin/tup.sh - $ACCUMULO_USER");
  } elsif ($am_accumulo_user) {
    # restart the as them as the accumulo user
    system("$ACCUMULO_HOME/bin/tup.sh");
  } else {
    # Not the accumulo user, try to sudo to the accumulo user
    system("sudo -u $ACCUMULO_USER $ACCUMULO_HOME/bin/tup.sh");
  }

  if ($kill_datanode) {
    print STDERR "$t Starting datanode on $server\n";
    if ($am_root) {
      # We're root, switch to the HDFS user
      system("ssh $server 'su -c \"$HADOOP_PREFIX/sbin/hadoop-daemon.sh start datanode\" - $HDFS_USER 2>/dev/null 1>/dev/null'");
    } elsif ($am_hdfs_user) {
      # We can just start as we're the HDFS user
      system("ssh $server '$HADOOP_PREFIX/sbin/hadoop-daemon.sh start datanode'");
    } else {
      # Not the HDFS user, have to try sudo
      system("ssh $server 'sudo -u $HDFS_USER $HADOOP_PREFIX/sbin/hadoop-daemon.sh start datanode'");
    }
  }

  $nextsleep1 = int(rand($sleep1max - $sleep1)) + $sleep1;
  sleep($nextsleep1 * 60);
}

