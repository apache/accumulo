Apache Accumulo Random Walk Tests
=================================

The randomwalk framework needs to be configured for your Accumulo instance by
doing the following steps:

1.  Make sure you have both `ACCUMULO_HOME` and `HADOOP_HOME` set in your
    `$ACCUMULO_CONF_DIR/accumulo-env.sh`.

2.  Create 'randomwalk.conf' file in the `conf` directory containing settings
    needed by walkers to connect to Accumulo.

3.  Create a 'walkers' file in the `conf` directory containing the hostnames of
    the machines where you want random walkers to run.

3.  Create a 'logger.xml' file in the `conf` directory from `logger.xml.example`.

The command below starts random walkers on all machines listed in 'walkers'.
The argument `Image.xml` indicates the module to use (which is located at
`conf/modules/Image.xml`):

> `$ ./bin/start-all.sh Image.xml`

All modules must be in `conf/modules` and can be referenced without this prefix.
For example, a module located at `conf/modules/foo/bar.xml` is started as
the following:

> `$ ./bin/start-all.sh foo/bar.xml`

This command will load all configuration in the `conf` directory to HDFS and
start identical random walkers on each node.  These random walkers will
download the current configuration from HDFS and place them in the `tmp/`
directory.

Random walkers will drop their logs in the `logs/` directory.  If you are running
multiple walkers and want ERROR/WARNs dropped to an NFS-hosted log, please set
`NFS_LOGPATH` to a NFS-mounted directory and uncomment the NFS appender in `logger.xml`.

You can kill all walkers on the machines listed in the 'walkers' file using
the following command:

> `$ ./bin/kill-all.sh`

Other Useful Commands
---------------------

Copies configuration in `conf/` to HDFS:

> `$ copy-config.sh`

Copies configuration from HDFS into `tmp/` and starts only one local random walker.

> `$ start-local.sh All.xml`

Stops all local random walkers:

> `$ pkill -f randomwalk.Framework`

Known Issues
------------

If you are running randomwalk tests while exercising Hadoop's high availability
(HA) failover capabilities, you should use Hadoop version 2.1.0 or later.
Failover scenarios are more likely to cause randomwalk test failures under
earlier Hadoop versions. See the following issue reports for more details.

* [HDFS-4404](https://issues.apache.org/jira/browse/HDFS-4404)
* [HADOOP-9792](https://issues.apache.org/jira/browse/HADOOP-9792)

