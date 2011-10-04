#! /usr/bin/env bash
#
# This script executes a program that will forward some or all of the logs to a running instance of Chainsaw v2.
# To use this script, start Chainsaw on a host and create a new XMLSocketReceiver. This script
# accepts the following command line parameters
#
#	host [required] - host running Chainsaw. Must be accessible via the network from this server
#   port [required] - port that XMLSocketReceiver is listening on.
#   filter [optional] - filter for log file names, * and ? are valid wildcards
#   start [optional] - filter log messages beginning at this time (format is yyyyMMddHHmmss)
#   end [optional] - filter log messages ending at this time (default is now, format is yyyyMMddHHmmss)
#   level [optional] - filter log messages with this level and higher
#   regex [optional] - filter log messages that match this regex (follows java.util.regex.Pattern syntax)
#
#
# Example:
#
#	LogForwarder.sh -h 127.0.0.1 -p 4448 -f tserver* -s 2010010100001 -e 20100101235959 -l INFO -m .*scan.*
#
. accumulo-config.sh

java -cp $ACCUMULO_HOME/lib org.apache.accumulo.server.util.SendLogToChainsaw -d $ACCUMULO_LOG_DIR "$@"
