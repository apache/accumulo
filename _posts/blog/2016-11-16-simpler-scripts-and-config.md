---
title: "Simpler scripts and configuration coming in Accumulo 2.0.0"
author: Mike Walch
reviewers: Josh Elser, Chistopher Tubbs
---

For the upcoming 2.0.0 release, Accumulo's scripts and configuration [were refactored][ACCUMULO-4490]
to make Accumulo easier to use. While Accumulo's documentation (i.e. the user
manual and [INSTALL.md]) were updated with any changes that were made, this blog post provides
a summary of the changes.

### Fewer scripts

Before 2.0.0, the `bin/` directory of Accumulo's binary tarball contained about 20 scripts:

```bash
$ ls accumulo-1.8.0/bin/
accumulo             build_native_library.sh  generate_monitor_certificate.sh  start-here.sh    stop-server.sh
accumulo_watcher.sh  check-slaves             LogForwarder.sh                  start-server.sh  tdown.sh
bootstrap_config.sh  config-server.sh         start-all.sh                     stop-all.sh      tool.sh
bootstrap_hdfs.sh    config.sh                start-daemon.sh                  stop-here.sh     tup.sh
```

The number of scripts made it difficult to know which scripts to use.  If you added the `bin/` directory to your 
`PATH`, it could add unecessary commands to your PATH or cause commands to be overriden due generic names
(like 'start-all.sh'). The number of scripts were reduced using the following methods:

* Scripts that are only called by other scripts were moved to a new `libexec/` directory in the Accumulo binary tarball
* Scripts with similiar functionality were combined
* Extra/optional scripts were move to a new `contrib/` directory in the binary tarball

Starting with 2.0.0, Accumulo will only have 3 scripts in its `bin/` directory:

```bash
$ ls accumulo-2.0.0/bin/
accumulo  accumulo-cluster  accumulo-service
```

Below are some notes on this change:

* The 'accumulo' script was left alone except for improved usage and the addition of 'create-config' and 'build-native'
  commands to replace 'bootstrap_config.sh' and 'build_native_library.sh'.
* The 'accumulo-service' script was created to manage Accumulo processes as services
* The 'accumulo-cluster' command was created to manage Accumulo on cluster and replaces 'start-all.sh' and 'stop-all.sh'.
* All optional scripts in `bin/` were moved to `contrib/`:

      $ ls accumulo-2.0.0/contrib/
      bootstrap-hdfs.sh  check-tservers  gen-monitor-cert.sh  tool.sh

### Less configuration

Before 2.0.0, Accumulo's `conf/` directory looked like the following (after creating initial config files
using 'bootstrap_config.sh'):

```bash
$ ls accumulo-1.8.0/conf/
accumulo-env.sh          auditLog.xml  generic_logger.properties            masters                    slaves
accumulo-metrics.xml     client.conf   generic_logger.xml                   monitor                    templates
accumulo.policy.example  examples      hadoop-metrics2-accumulo.properties  monitor_logger.properties  tracers
accumulo-site.xml        gc            log4j.properties                     monitor_logger.xml
```

While all of these files have a purpose, many are only used in rare situations. Therefore, the
'accumulo create-config' (which replaces 'bootstrap_config.sh') now only generates a minimum
set of configuration files needed to run Accumulo.

```bash
$ cd accumulo-2.0.0
$ ./bin/accumulo create-config
$ ls conf/
accumulo-env.sh  accumulo-site.xml  client.conf  examples
```

The 'accumulo create-config' command does not generate host files (i.e 'tservers', 'monitor', etc) to run processes locally.
These files are only required by the 'accumulo-cluster' command which has a command to generate them.

```bash
$ cd accumulo-2.0.0/
$ ./bin/accumulo-cluster create-config
$ ls conf/
accumulo-env.sh  accumulo-site.xml  client.conf  examples  gc  masters  monitor  tracers  tservers
```

Any less common configuration files that were not generated above can still be found in `conf/examples`.

### Better usage

Before 2.0.0, the 'accumulo' command had a limited usage:

```
$ ./accumulo-1.8.0/bin/accumulo
accumulo admin | check-server-config | classpath | create-token | gc | help | info | init | jar <jar> [<main class>] args |
  login-info | master | minicluster | monitor | proxy | rfile-info | shell | tracer | tserver | version | zookeeper | <accumulo class> args
```

For 2.0.0, all 'accumulo' commands were given a short description and organized into the groups.  Below is
the full usage. It should be noted that usage is limited until the 'accumulo-env.sh' configuration file is
created in `conf/` by the `accumulo create-config` command.

```
$ ./accumulo-2.0.0/bin/accumulo help

Usage: accumulo <command> (<argument> ...)

Core Commands:
  create-config                  Creates Accumulo configuration
  build-native                   Builds Accumulo native libraries
  init                           Initializes Accumulo
  shell                          Runs Accumulo shell
  classpath                      Prints Accumulo classpath
  version                        Prints Accumulo version
  admin                          Executes administrative commands
  info                           Prints Accumulo cluster info
  help                           Prints usage
  jar <jar> [<main class>] args  Runs Java <main class> in <jar> using Accumulo classpath
  <main class> args              Runs Java <main class> located on Accumulo classpath

Process Commands:
  gc                             Starts Accumulo garbage collector
  master                         Starts Accumulo master
  monitor                        Starts Accumulo monitor
  minicluster                    Starts Accumulo minicluster
  proxy                          Starts Accumulo proxy
  tserver                        Starts Accumulo tablet server
  tracer                         Starts Accumulo tracer
  zookeeper                      Starts Apache Zookeeper instance

Advanced Commands:
  check-server-config            Checks server config
  create-token                   Creates authentication token
  login-info                     Prints Accumulo login info
  rfile-info                     Prints rfile info
```

The new 'accumulo-service' and 'accumulo-cluster' commands also have informative usage.

```
$ ./accumulo-2.0.0/bin/accumulo-service 

Usage: accumulo-service <service> <command>

Services:
  gc          Accumulo garbage collector
  monitor     Accumulo monitor
  master      Accumulo master
  tserver     Accumulo tserver
  tracer      Accumulo tracter

Commands:
  start       Starts service
  stop        Stops service
  kill        Kills service

$ ./accumulo-2.0.0/bin/accumulo-cluster 

Usage: accumulo-cluster <command> (<argument> ...)

Commands:
  create-config   Creates cluster config
  start           Starts Accumulo cluster
  stop            Stops Accumulo cluster
```

[ACCUMULO-4490]: https://issues.apache.org/jira/browse/ACCUMULO-4490
[INSTALL.md]: https://github.com/apache/accumulo/blob/master/INSTALL.md
