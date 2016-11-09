---
title: "User Manual: Shell Commands"
---

** Up:** [Apache Accumulo User Manual Version 1.3][3] ** Previous:** [Administration][5]   ** [Contents][7]**   
  


## <a id="Shell_Commands"></a> Shell Commands

**?**   
  
    usage: ? [ <command> <command> ] [-?] [-np]   
    description: provides information about the available commands   
      -?,-help  display this help   
      -np,-no-pagination  disables pagination of output   
  
**about**   
  
    usage: about [-?] [-v]   
    description: displays information about this program   
      -?,-help  display this help   
      -v,-verbose displays details session information   
  
**addsplits**   
  
    usage: addsplits [<split> <split> ] [-?] [-b64] [-sf <filename>] -t <tableName>   
    description: add split points to an existing table   
      -?,-help  display this help   
      -b64,-base64encoded decode encoded split points   
      -sf,-splits-file <filename> file with newline separated list of rows to add   
           to table   
      -t,-table <tableName>  name of a table to add split points to   
  
**authenticate**   
  
    usage: authenticate <username> [-?]   
    description: verifies a user's credentials   
      -?,-help  display this help   
  
**bye**   
  
    usage: bye [-?]   
    description: exits the shell   
      -?,-help  display this help   
  
**classpath**   
  
    usage: classpath [-?]   
    description: lists the current files on the classpath   
      -?,-help  display this help   
  
**clear**   
  
    usage: clear [-?]   
    description: clears the screen   
      -?,-help  display this help   
  
**cls**   
  
    usage: cls [-?]   
    description: clears the screen   
      -?,-help  display this help   
  
**compact**   
  
    usage: compact [-?] [-override] -p <pattern> | -t <tableName>   
    description: sets all tablets for a table to major compact as soon as possible   
           (based on current time)   
      -?,-help  display this help   
      -override  override a future scheduled compaction   
      -p,-pattern <pattern>  regex pattern of table names to flush   
      -t,-table <tableName>  name of a table to flush   
  
**config**   
  
    usage: config [-?] [-d <property> | -f <string> | -s <property=value>] [-np]   
           [-t <table>]   
    description: prints system properties and table specific properties   
      -?,-help  display this help   
      -d,-delete <property>  delete a per-table property   
      -f,-filter <string> show only properties that contain this string   
      -np,-no-pagination  disables pagination of output   
      -s,-set <property=value>  set a per-table property   
      -t,-table <table>  display/set/delete properties for specified table   
  
**createtable**   
  
    usage: createtable <tableName> [-?] [-a   
           <<columnfamily>[:<columnqualifier>]=<aggregation_class>>] [-b64]   
           [-cc <table>] [-cs <table> | -sf <filename>] [-ndi]  [-tl | -tm]   
    description: creates a new table, with optional aggregators and optionally   
           pre-split   
      -?,-help  display this help   
      -a,-aggregator <<columnfamily>[:<columnqualifier>]=<aggregation_class>>   
           comma separated column=aggregator   
      -b64,-base64encoded decode encoded split points   
      -cc,-copy-config <table>  table to copy configuration from   
      -cs,-copy-splits <table>  table to copy current splits from   
      -ndi,-no-default-iterators  prevents creation of the normal default iterator   
           set   
      -sf,-splits-file <filename> file with newline separated list of rows to   
           create a pre-split table   
      -tl,-time-logical  use logical time   
      -tm,-time-millis  use time in milliseconds   
  
**createuser**   
  
    usage: createuser <username> [-?] [-s <comma-separated-authorizations>]   
    description: creates a new user   
      -?,-help  display this help   
      -s,-scan-authorizations <comma-separated-authorizations>  scan authorizations   
  
**debug**   
  
    usage: debug [ on | off ] [-?]   
    description: turns debug logging on or off   
      -?,-help  display this help   
  
**delete**   
  
    usage: delete <row> <colfamily> <colqualifier> [-?] [-l <expression>] [-t   
           <timestamp>]   
    description: deletes a record from a table   
      -?,-help  display this help   
      -l,-authorization-label <expression>  formatted authorization label expression   
      -t,-timestamp <timestamp>  timestamp to use for insert   
  
**deleteiter**   
  
    usage: deleteiter [-?] [-majc] [-minc] -n <itername> [-scan] [-t <table>]   
    description: deletes a table-specific iterator   
      -?,-help  display this help   
      -majc,-major-compaction  applied at major compaction   
      -minc,-minor-compaction  applied at minor compaction   
      -n,-name <itername> iterator to delete   
      -scan,-scan-time  applied at scan time   
      -t,-table <table>  tableName   
  
**deletemany**   
  
    usage: deletemany [-?] [-b <start-row>] [-c   
           <<columnfamily>[:<columnqualifier>]>] [-e <end-row>] [-f] [-np]   
           [-s <comma-separated-authorizations>] [-st]   
    description: scans a table and deletes the resulting records   
      -?,-help  display this help   
      -b,-begin-row <start-row>  begin row (inclusive)   
      -c,-columns <<columnfamily>[:<columnqualifier>]>  comma-separated columns   
      -e,-end-row <end-row>  end row (inclusive)   
      -f,-force  forces deletion without prompting   
      -np,-no-pagination  disables pagination of output   
      -s,-scan-authorizations <comma-separated-authorizations>  scan authorizations   
           (all user auths are used if this argument is not specified)   
      -st,-show-timestamps  enables displaying timestamps   
  
**deletescaniter**   
  
    usage: deletescaniter [-?] [-a] [-n <itername>] [-t <table>]   
    description: deletes a table-specific scan iterator so it is no longer used   
           during this shell session   
      -?,-help  display this help   
      -a,-all  delete all for tableName   
      -n,-name <itername> iterator to delete   
      -t,-table <table>  tableName   
  
**deletetable**   
  
    usage: deletetable <tableName> [-?]   
    description: deletes a table   
      -?,-help  display this help   
  
**deleteuser**   
  
    usage: deleteuser <username> [-?]   
    description: deletes a user   
      -?,-help  display this help   
  
**droptable**   
  
    usage: droptable <tableName> [-?]   
    description: deletes a table   
      -?,-help  display this help   
  
**dropuser**   
  
    usage: dropuser <username> [-?]   
    description: deletes a user   
      -?,-help  display this help   
  
**egrep**   
  
    usage: egrep <regex> <regex> [-?] [-b <start-row>] [-c   
           <<columnfamily>[:<columnqualifier>]>] [-e <end-row>] [-np] [-s   
           <comma-separated-authorizations>] [-st] [-t <arg>]   
    description: egreps a table in parallel on the server side (uses java regex)   
      -?,-help  display this help   
      -b,-begin-row <start-row>  begin row (inclusive)   
      -c,-columns <<columnfamily>[:<columnqualifier>]>  comma-separated columns   
      -e,-end-row <end-row>  end row (inclusive)   
      -np,-no-pagination  disables pagination of output   
      -s,-scan-authorizations <comma-separated-authorizations>  scan authorizations   
           (all user auths are used if this argument is not specified)   
      -st,-show-timestamps  enables displaying timestamps   
      -t,-num-threads <arg>  num threads   
  
**execfile**   
  
    usage: execfile [-?] [-v]   
    description: specifies a file containing accumulo commands to execute   
      -?,-help  display this help   
      -v,-verbose displays command prompt as commands are executed   
  
**exit**   
  
    usage: exit [-?]   
    description: exits the shell   
      -?,-help  display this help   
  
**flush**   
  
    usage: flush [-?] -p <pattern> | -t <tableName>   
    description: makes a best effort to flush tables from memory to disk   
      -?,-help  display this help   
      -p,-pattern <pattern>  regex pattern of table names to flush   
      -t,-table <tableName>  name of a table to flush   
  
**formatter**   
  
    usage: formatter [-?] -f <className> | -l | -r   
    description: specifies a formatter to use for displaying database entries   
      -?,-help  display this help   
      -f,-formatter <className>  fully qualified name of formatter class to use   
      -l,-list  display the current formatter   
      -r,-reset  reset to default formatter   
  
**getauths**   
  
    usage: getauths [-?] [-u <user>]   
    description: displays the maximum scan authorizations for a user   
      -?,-help  display this help   
      -u,-user <user>  user to operate on   
  
**getgroups**   
  
    usage: getgroups [-?] -t <table>   
    description: gets the locality groups for a given table   
      -?,-help  display this help   
      -t,-table <table>  get locality groups for specified table   
  
**getsplits**   
  
    usage: getsplits [-?] [-b64] [-m <num>] [-o <file>] [-v]   
    description: retrieves the current split points for tablets in the current table   
      -?,-help  display this help   
      -b64,-base64encoded encode the split points   
      -m,-max <num>  specifies the maximum number of splits to create   
      -o,-output <file>  specifies a local file to write the splits to   
      -v,-verbose print out the tablet information with start/end rows   
  
**grant**   
  
    usage: grant <permission> [-?] -p <pattern> | -s | -t <table>  -u <username>   
    description: grants system or table permissions for a user   
      -?,-help  display this help   
      -p,-pattern <pattern>  regex pattern of tables to grant permissions on   
      -s,-system  grant a system permission   
      -t,-table <table>  grant a table permission on this table   
      -u,-user <username> user to operate on   
  
**grep**   
  
    usage: grep <term> <term> [-?] [-b <start-row>] [-c   
           <<columnfamily>[:<columnqualifier>]>] [-e <end-row>] [-np] [-s   
           <comma-separated-authorizations>] [-st] [-t <arg>]   
    description: searches a table for a substring, in parallel, on the server side   
      -?,-help  display this help   
      -b,-begin-row <start-row>  begin row (inclusive)   
      -c,-columns <<columnfamily>[:<columnqualifier>]>  comma-separated columns   
      -e,-end-row <end-row>  end row (inclusive)   
      -np,-no-pagination  disables pagination of output   
      -s,-scan-authorizations <comma-separated-authorizations>  scan authorizations   
           (all user auths are used if this argument is not specified)   
      -st,-show-timestamps  enables displaying timestamps   
      -t,-num-threads <arg>  num threads   
  
**help**   
  
    usage: help [ <command> <command> ] [-?] [-np]   
    description: provides information about the available commands   
      -?,-help  display this help   
      -np,-no-pagination  disables pagination of output   
  
**importdirectory**   
  
    usage: importdirectory <directory> <failureDirectory> [-?] [-a <num>] [-f <num>]   
           [-g] [-v]   
    description: bulk imports an entire directory of data files to the current table   
      -?,-help  display this help   
      -a,-numAssignThreads <num>  number of assign threads for import (default: 20)   
      -f,-numFileThreads <num>  number of threads to process files (default: 8)   
      -g,-disableGC  prevents imported files from being deleted by the garbage   
           collector   
      -v,-verbose displays statistics from the import   
  
**info**   
  
    usage: info [-?] [-v]   
    description: displays information about this program   
      -?,-help  display this help   
      -v,-verbose displays details session information   
  
**insert**   
  
    usage: insert <row> <colfamily> <colqualifier> <value> [-?] [-l <expression>] [-t   
           <timestamp>]   
    description: inserts a record   
      -?,-help  display this help   
      -l,-authorization-label <expression>  formatted authorization label expression   
      -t,-timestamp <timestamp>  timestamp to use for insert   
  
**listscans**   
  
    usage: listscans [-?] [-np] [-ts <tablet server>]   
    description: list what scans are currently running in accumulo. See the   
           org.apache.accumulo.core.client.admin.ActiveScan javadoc for more information   
           about columns.   
      -?,-help  display this help   
      -np,-no-pagination  disables pagination of output   
      -ts,-tabletServer <tablet server>  list scans for a specific tablet server   
  
**masterstate**   
  
    usage: masterstate <NORMAL|SAFE_MODE|CLEAN_STOP> [-?]   
    description: set the master state: NORMAL, SAFE_MODE or CLEAN_STOP   
      -?,-help  display this help   
  
**offline**   
  
    usage: offline [-?] -p <pattern> | -t <tableName>   
    description: starts the process of taking table offline   
      -?,-help  display this help   
      -p,-pattern <pattern>  regex pattern of table names to flush   
      -t,-table <tableName>  name of a table to flush   
  
**online**   
  
    usage: online [-?] -p <pattern> | -t <tableName>   
    description: starts the process of putting a table online   
      -?,-help  display this help   
      -p,-pattern <pattern>  regex pattern of table names to flush   
      -t,-table <tableName>  name of a table to flush   
  
**passwd**   
  
    usage: passwd [-?] [-u <user>]   
    description: changes a user's password   
      -?,-help  display this help   
      -u,-user <user>  user to operate on   
  
**quit**   
  
    usage: quit [-?]   
    description: exits the shell   
      -?,-help  display this help   
  
**renametable**   
  
    usage: renametable <current table name> <new table name> [-?]   
    description: rename a table   
      -?,-help  display this help   
  
**revoke**   
  
    usage: revoke <permission> [-?] -s | -t <table>  -u <username>   
    description: revokes system or table permissions from a user   
      -?,-help  display this help   
      -s,-system  revoke a system permission   
      -t,-table <table>  revoke a table permission on this table   
      -u,-user <username> user to operate on   
  
**scan**   
  
    usage: scan [-?] [-b <start-row>] [-c <<columnfamily>[:<columnqualifier>]>] [-e   
           <end-row>] [-np] [-s <comma-separated-authorizations>] [-st]   
    description: scans the table, and displays the resulting records   
      -?,-help  display this help   
      -b,-begin-row <start-row>  begin row (inclusive)   
      -c,-columns <<columnfamily>[:<columnqualifier>]>  comma-separated columns   
      -e,-end-row <end-row>  end row (inclusive)   
      -np,-no-pagination  disables pagination of output   
      -s,-scan-authorizations <comma-separated-authorizations>  scan authorizations   
           (all user auths are used if this argument is not specified)   
      -st,-show-timestamps  enables displaying timestamps   
  
**select**   
  
    usage: select <row> <columnfamily> <columnqualifier> [-?] [-np] [-s   
           <comma-separated-authorizations>] [-st]   
    description: scans for and displays a single record   
      -?,-help  display this help   
      -np,-no-pagination  disables pagination of output   
      -s,-scan-authorizations <comma-separated-authorizations>  scan authorizations   
      -st,-show-timestamps  enables displaying timestamps   
  
**selectrow**   
  
    usage: selectrow <row> [-?] [-np] [-s <comma-separated-authorizations>] [-st]   
    description: scans a single row and displays all resulting records   
      -?,-help  display this help   
      -np,-no-pagination  disables pagination of output   
      -s,-scan-authorizations <comma-separated-authorizations>  scan authorizations   
      -st,-show-timestamps  enables displaying timestamps   
  
**setauths**   
  
    usage: setauths [-?] -c | -s <comma-separated-authorizations>  [-u <user>]   
    description: sets the maximum scan authorizations for a user   
      -?,-help  display this help   
      -c,-clear-authorizations  clears the scan authorizations   
      -s,-scan-authorizations <comma-separated-authorizations>  set the scan   
           authorizations   
      -u,-user <user>  user to operate on   
  
**setgroups**   
  
    usage: setgroups <group>=<col fam>,<col fam> <group>=<col fam>,<col fam>   
           [-?] -t <table>   
    description: sets the locality groups for a given table (for binary or commas,   
           use Java API)   
      -?,-help  display this help   
      -t,-table <table>  get locality groups for specified table   
  
**setiter**   
  
    usage: setiter [-?] -agg | -class <name> | -filter | -nolabel | -regex | -vers   
           [-majc] [-minc] [-n <itername>]  -p <pri>  [-scan] [-t <table>]   
    description: sets a table-specific iterator   
      -?,-help  display this help   
      -agg,-aggregator  an aggregating type   
      -class,-class-name <name>  a java class type   
      -filter,-filter  a filtering type   
      -majc,-major-compaction  applied at major compaction   
      -minc,-minor-compaction  applied at minor compaction   
      -n,-name <itername> iterator to set   
      -nolabel,-no-label  a no-labeling type   
      -p,-priority <pri>  the order in which the iterator is applied   
      -regex,-regular-expression  a regex matching type   
      -scan,-scan-time  applied at scan time   
      -t,-table <table>  tableName   
      -vers,-version  a versioning type   
  
**setscaniter**   
  
    usage: setscaniter [-?] -agg | -class <name> | -filter | -nolabel | -regex |   
           -vers  [-n <itername>]  -p <pri> [-t <table>]   
    description: sets a table-specific scan iterator for this shell session   
      -?,-help  display this help   
      -agg,-aggregator  an aggregating type   
      -class,-class-name <name>  a java class type   
      -filter,-filter  a filtering type   
      -n,-name <itername> iterator to set   
      -nolabel,-no-label  a no-labeling type   
      -p,-priority <pri>  the order in which the iterator is applied   
      -regex,-regular-expression  a regex matching type   
      -t,-table <table>  tableName   
      -vers,-version  a versioning type   
  
**systempermissions**   
  
    usage: systempermissions [-?]   
    description: displays a list of valid system permissions   
      -?,-help  display this help   
  
**table**   
  
    usage: table <tableName> [-?]   
    description: switches to the specified table   
      -?,-help  display this help   
  
**tablepermissions**   
  
    usage: tablepermissions [-?]   
    description: displays a list of valid table permissions   
      -?,-help  display this help   
  
**tables**   
  
    usage: tables [-?] [-l]   
    description: displays a list of all existing tables   
      -?,-help  display this help   
      -l,-list-ids  display internal table ids along with the table name   
  
**trace**   
  
    usage: trace [ on | off ] [-?]   
    description: turns trace logging on or off   
      -?,-help  display this help   
  
**user**   
  
    usage: user <username> [-?]   
    description: switches to the specified user   
      -?,-help  display this help   
  
**userpermissions**   
  
    usage: userpermissions [-?] [-u <user>]   
    description: displays a user's system and table permissions   
      -?,-help  display this help   
      -u,-user <user>  user to operate on   
  
**users**   
  
    usage: users [-?]   
    description: displays a list of existing users   
      -?,-help  display this help   
  
**whoami**   
  
    usage: whoami [-?]   
    description: reports the current user name   
      -?,-help  display this help   
  
  


* * *

** Up:** [Apache Accumulo User Manual Version 1.3][3] ** Previous:** [Administration][5]   ** [Contents][7]**

[3]: accumulo_user_manual.html
[5]: Administration.html
[7]: Contents.html

