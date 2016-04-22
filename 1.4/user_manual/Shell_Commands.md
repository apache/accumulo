---
title: "User Manual: Shell Commands"
---

** Up:** [Apache Accumulo User Manual Version 1.4][3] ** Previous:** [Administration][5]   ** [Contents][7]**   
  


## <a id="Shell_Commands"></a> Shell Commands

  
**?**   
  
    usage: ? [ <command> <command> ] [-?] [-np] [-nw]   
    description: provides information about the available commands   
      -?,-help  display this help   
      -np,-no-pagination  disables pagination of output   
      -nw,-no-wrap  disables wrapping of output   
  
**about**   
  
    usage: about [-?] [-v]   
    description: displays information about this program   
      -?,-help  display this help   
      -v,-verbose  displays details session information   
  
**addsplits**   
  
    usage: addsplits [<split> <split> ] [-?] [-b64] [-sf <filename>] [-t <tableName>]   
    description: add split points to an existing table   
      -?,-help  display this help   
      -b64,-base64encoded  decode encoded split points   
      -sf,-splits-file <filename>  file with newline separated list of rows to add to   
              table   
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
  
**clonetable**   
  
    usage: clonetable <current table name> <new table name> [-?] [-e <arg>] [-nf] [-s   
              <arg>]   
    description: clone a table   
      -?,-help  display this help   
      -e,-exclude <arg>  properties that should not be copied from source table.   
              Expects <prop>,<prop>   
      -nf,-noFlush  do not flush table data in memory before cloning.   
      -s,-set <arg>  set initial properties before the table comes online. Expects   
              <prop>=<value>,<prop>=<value>   
  
**cls**   
  
    usage: cls [-?]   
    description: clears the screen   
      -?,-help  display this help   
  
**compact**   
  
    usage: compact [-?] [-b <arg>] [-e <arg>] [-nf] [-p <pattern> | -t <tableName>]   
              [-w]   
    description: sets all tablets for a table to major compact as soon as possible   
              (based on current time)   
      -?,-help  display this help   
      -b,-begin-row <arg>  begin row   
      -e,-end-row <arg>  end row   
      -nf,-noFlush  do not flush table data in memory before compacting.   
      -p,-pattern <pattern>  regex pattern of table names to flush   
      -t,-table <tableName>  name of a table to flush   
      -w,-wait  wait for compact to finish   
  
**config**   
  
    usage: config [-?] [-d <property> | -f <string> | -s <property=value>]  [-np]  [-t   
              <table>]   
    description: prints system properties and table specific properties   
      -?,-help  display this help   
      -d,-delete <property>  delete a per-table property   
      -f,-filter <string>  show only properties that contain this string   
      -np,-no-pagination  disables pagination of output   
      -s,-set <property=value>  set a per-table property   
      -t,-table <table>  display/set/delete properties for specified table   
  
**createtable**   
  
    usage: createtable <tableName> [-?] [-a   
              <<columnfamily>[:<columnqualifier>]=<aggregation class>>] [-b64] [-cc   
              <table>] [-cs <table> | -sf <filename>] [-evc] [-f <className>] [-ndi]   
              [-tl | -tm]   
    description: creates a new table, with optional aggregators and optionally pre-split   
      -?,-help  display this help   
      -a,-aggregator <<columnfamily>[:<columnqualifier>]=<aggregation class>>  comma   
              separated column=aggregator   
      -b64,-base64encoded  decode encoded split points   
      -cc,-copy-config <table>  table to copy configuration from   
      -cs,-copy-splits <table>  table to copy current splits from   
      -evc,-enable-visibility-constraint  prevents users from writing data they can not   
              read.  When enabling this may want to consider disabling bulk import and   
              alter table   
      -f,-formatter <className>  default formatter to set   
      -ndi,-no-default-iterators  prevents creation of the normal default iterator set   
      -sf,-splits-file <filename>  file with newline separated list of rows to create a   
              pre-split table   
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
      -n,-name <itername>  iterator to delete   
      -scan,-scan-time  applied at scan time   
      -t,-table <table>  tableName   
  
**deletemany**   
  
    usage: deletemany [-?] [-b <start-row>] [-c   
              «columnfamily>[:<columnqualifier>],<columnfamily>[:<columnqualifier>]>]   
              [-e <end-row>] [-f] [-fm <className>] [-np] [-r <row>] [-s   
              <comma-separated-authorizations>] [-st] [-t <table>]   
    description: scans a table and deletes the resulting records   
      -?,-help  display this help   
      -b,-begin-row <start-row>  begin row (inclusive)   
      -c,-columns   
              «columnfamily>[:<columnqualifier>],<columnfamily>[:<columnqualifier>]>   
              comma-separated columns   
      -e,-end-row <end-row>  end row (inclusive)   
      -f,-force  forces deletion without prompting   
      -fm,-formatter <className>  fully qualified name of the formatter class to use   
      -np,-no-pagination  disables pagination of output   
      -r,-row <row>  row to scan   
      -s,-scan-authorizations <comma-separated-authorizations>  scan authorizations   
              (all user auths are used if this argument is not specified)   
      -st,-show-timestamps  enables displaying timestamps   
      -t,-table <table>  table to be created   
  
**deleterows**   
  
    usage: deleterows [-?] [-b <arg>] [-e <arg>] [-f] [-t <table>]   
    description: delete a range of rows in a table.  Note that rows matching the start   
              row ARE NOT deleted, but rows matching the end row ARE deleted.   
      -?,-help  display this help   
      -b,-begin-row <arg>  begin row   
      -e,-end-row <arg>  end row   
      -f,-force  delete data even if start or end are not specified   
      -t,-tableName <table>  table to delete row range   
  
**deletescaniter**   
  
    usage: deletescaniter [-?] [-a] [-n <itername>] [-t <table>]   
    description: deletes a table-specific scan iterator so it is no longer used during   
              this shell session   
      -?,-help  display this help   
      -a,-all  delete all for tableName   
      -n,-name <itername>  iterator to delete   
      -t,-table <table>  tableName   
  
**deletetable**   
  
    usage: deletetable <tableName> [-?] [-t <arg>]   
    description: deletes a table   
      -?,-help  display this help   
      -t,-tableName <arg>  deletes a table   
  
**deleteuser**   
  
    usage: deleteuser <username> [-?]   
    description: deletes a user   
      -?,-help  display this help   
  
**droptable**   
  
    usage: droptable <tableName> [-?] [-t <arg>]   
    description: deletes a table   
      -?,-help  display this help   
      -t,-tableName <arg>  deletes a table   
  
**dropuser**   
  
    usage: dropuser <username> [-?]   
    description: deletes a user   
      -?,-help  display this help   
  
**du**   
  
    usage: du <table> <table> [-?] [-p <pattern>]   
    description: Prints how much space is used by files referenced by a table.  When   
              multiple tables are specified it prints how much space is used by files   
              shared between tables, if any.   
      -?,-help  display this help   
      -p,-pattern <pattern>  regex pattern of table names   
  
**egrep**   
  
    usage: egrep <regex> <regex> [-?] [-b <start-row>] [-c   
              «columnfamily>[:<columnqualifier>],<columnfamily>[:<columnqualifier>]>]   
              [-e <end-row>] [-f <int>] [-fm <className>] [-np] [-nt <arg>] [-r <row>]   
              [-s <comma-separated-authorizations>] [-st] [-t <table>]   
    description: searches each row, column family, column qualifier and value, in   
              parallel, on the server side (using a java Matcher, so put .* before and   
              after your term if you're not matching the whole element)   
      -?,-help  display this help   
      -b,-begin-row <start-row>  begin row (inclusive)   
      -c,-columns   
              «columnfamily>[:<columnqualifier>],<columnfamily>[:<columnqualifier>]>   
              comma-separated columns   
      -e,-end-row <end-row>  end row (inclusive)   
      -f,-show few <int>  Only shows certain amount of characters   
      -fm,-formatter <className>  fully qualified name of the formatter class to use   
      -np,-no-pagination  disables pagination of output   
      -nt,-num-threads <arg>  num threads   
      -r,-row <row>  row to scan   
      -s,-scan-authorizations <comma-separated-authorizations>  scan authorizations   
              (all user auths are used if this argument is not specified)   
      -st,-show-timestamps  enables displaying timestamps   
      -t,-tableName <table>  table to grep through   
  
**execfile**   
  
    usage: execfile [-?] [-v]   
    description: specifies a file containing accumulo commands to execute   
      -?,-help  display this help   
      -v,-verbose  displays command prompt as commands are executed   
  
**exit**   
  
    usage: exit [-?]   
    description: exits the shell   
      -?,-help  display this help   
  
**flush**   
  
    usage: flush [-?] [-b <arg>] [-e <arg>] [-p <pattern> | -t <tableName>]  [-w]   
    description: flushes a tables data that is currently in memory to disk   
      -?,-help  display this help   
      -b,-begin-row <arg>  begin row   
      -e,-end-row <arg>  end row   
      -p,-pattern <pattern>  regex pattern of table names to flush   
      -t,-table <tableName>  name of a table to flush   
      -w,-wait  wait for flush to finish   
  
**formatter**   
  
    usage: formatter [-?] -f <className> | -l | -r  [-t <table>]   
    description: specifies a formatter to use for displaying table entries   
      -?,-help  display this help   
      -f,-formatter <className>  fully qualified name of the formatter class to use   
      -l,-list  display the current formatter   
      -r,-remove  remove the current formatter   
      -t,-table <table>  table to set the formatter on   
  
**getauths**   
  
    usage: getauths [-?] [-u <user>]   
    description: displays the maximum scan authorizations for a user   
      -?,-help  display this help   
      -u,-user <user>  user to operate on   
  
**getgroups**   
  
    usage: getgroups [-?] [-t <table>]   
    description: gets the locality groups for a given table   
      -?,-help  display this help   
      -t,-table <table>  get locality groups for specified table   
  
**getsplits**   
  
    usage: getsplits [-?] [-b64] [-m <num>] [-o <file>] [-t <table>] [-v]   
    description: retrieves the current split points for tablets in the current table   
      -?,-help  display this help   
      -b64,-base64encoded  encode the split points   
      -m,-max <num>  specifies the maximum number of splits to create   
      -o,-output <file>  specifies a local file to write the splits to   
      -t,-tableName <table>  table to get splits on   
      -v,-verbose  print out the tablet information with start/end rows   
  
**grant**   
  
    usage: grant <permission> [-?] -p <pattern> | -s | -t <table>  -u <username>   
    description: grants system or table permissions for a user   
      -?,-help  display this help   
      -p,-pattern <pattern>  regex pattern of tables to grant permissions on   
      -s,-system  grant a system permission   
      -t,-table <table>  grant a table permission on this table   
      -u,-user <username>  user to operate on   
  
**grep**   
  
    usage: grep <term> <term> [-?] [-b <start-row>] [-c   
              «columnfamily>[:<columnqualifier>],<columnfamily>[:<columnqualifier>]>]   
              [-e <end-row>] [-f <int>] [-fm <className>] [-np] [-nt <arg>] [-r <row>]   
              [-s <comma-separated-authorizations>] [-st] [-t <table>]   
    description: searches each row, column family, column qualifier and value in a table   
              for a substring (not a regular expression), in parallel, on the server   
              side   
      -?,-help  display this help   
      -b,-begin-row <start-row>  begin row (inclusive)   
      -c,-columns   
              «columnfamily>[:<columnqualifier>],<columnfamily>[:<columnqualifier>]>   
              comma-separated columns   
      -e,-end-row <end-row>  end row (inclusive)   
      -f,-show few <int>  Only shows certain amount of characters   
      -fm,-formatter <className>  fully qualified name of the formatter class to use   
      -np,-no-pagination  disables pagination of output   
      -nt,-num-threads <arg>  num threads   
      -r,-row <row>  row to scan   
      -s,-scan-authorizations <comma-separated-authorizations>  scan authorizations   
              (all user auths are used if this argument is not specified)   
      -st,-show-timestamps  enables displaying timestamps   
      -t,-tableName <table>  table to grep through   
  
**help**   
  
    usage: help [ <command> <command> ] [-?] [-np] [-nw]   
    description: provides information about the available commands   
      -?,-help  display this help   
      -np,-no-pagination  disables pagination of output   
      -nw,-no-wrap  disables wrapping of output   
  
**history**   
  
    usage: history [-?] [-c]   
    description: Generates a list of commands previously executed   
      -?,-help  display this help   
      -c,-Clears History, takes no arguments.  Clears History File   
  
**importdirectory**   
  
    usage: importdirectory <directory> <failureDirectory> true|false [-?]   
    description: bulk imports an entire directory of data files to the current table.   
              The boolean argument determines if accumulo sets the time.   
      -?,-help  display this help   
  
**info**   
  
    usage: info [-?] [-v]   
    description: displays information about this program   
      -?,-help  display this help   
      -v,-verbose  displays details session information   
  
**insert**   
  
    usage: insert <row> <colfamily> <colqualifier> <value> [-?] [-l <expression>] [-t   
              <timestamp>]   
    description: inserts a record   
      -?,-help  display this help   
      -l,-authorization-label <expression>  formatted authorization label expression   
      -t,-timestamp <timestamp>  timestamp to use for insert   
  
**listiter**   
  
    usage: listiter [-?] [-majc] [-minc] [-n <itername>] [-scan] [-t <table>]   
    description: lists table-specific iterators   
      -?,-help  display this help   
      -majc,-major-compaction  applied at major compaction   
      -minc,-minor-compaction  applied at minor compaction   
      -n,-name <itername>  iterator to delete   
      -scan,-scan-time  applied at scan time   
      -t,-table <table>  tableName   
  
**listscans**   
  
    usage: listscans [-?] [-np] [-ts <tablet server>]   
    description: list what scans are currently running in accumulo. See the   
              accumulo.core.client.admin.ActiveScan javadoc for more information about   
              columns.   
      -?,-help  display this help   
      -np,-no-pagination  disables pagination of output   
      -ts,-tabletServer <tablet server>  list scans for a specific tablet server   
  
**masterstate**   
  
    usage: masterstate is deprecated, use the command line utility instead [-?]   
    description: DEPRECATED: use the command line utility instead   
      -?,-help  display this help   
  
**maxrow**   
  
    usage: maxrow [-?] [-b <begin-row>] [-be] [-e <end-row>] [-ee] [-s   
              <comma-separated-authorizations>] [-t <table>]   
    description: find the max row in a table within a given range   
      -?,-help  display this help   
      -b,-begin-row <begin-row>  begin row   
      -be,-begin-exclusive  make start row exclusive, by defaults it inclusive   
      -e,-end-row <end-row>  end row   
      -ee,-end-exclusive  make end row exclusive, by defaults it inclusive   
      -s,-scan-authorizations <comma-separated-authorizations>  scan authorizations   
              (all user auths are used if this argument is not specified)   
      -t,-table <table>  table to be created   
  
**merge**   
  
    usage: merge [-?] [-b <arg>] [-e <arg>] [-f] [-s <arg>] [-t <table>] [-v]   
    description: merge tablets in a table   
      -?,-help  display this help   
      -b,-begin-row <arg>  begin row   
      -e,-end-row <arg>  end row   
      -f,-force  merge small tablets to large tablets, even if it goes over the given   
              size   
      -s,-size <arg>  merge tablets to the given size over the entire table   
      -t,-tableName <table>  table to be merged   
      -v,-verbose  verbose output during merge   
  
**notable**   
  
    usage: notable [-?] [-t <arg>]   
    description: returns to a tableless shell state   
      -?,-help  display this help   
      -t,-tableName <arg>  Returns to a no table state   
  
**offline**   
  
    usage: offline [-?] [-p <pattern> | -t <tableName>]   
    description: starts the process of taking table offline   
      -?,-help  display this help   
      -p,-pattern <pattern>  regex pattern of table names to flush   
      -t,-table <tableName>  name of a table to flush   
  
**online**   
  
    usage: online [-?] [-p <pattern> | -t <tableName>]   
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
      -u,-user <username>  user to operate on   
  
**scan**   
  
    usage: scan [-?] [-b <start-row>] [-c   
              «columnfamily>[:<columnqualifier>],<columnfamily>[:<columnqualifier>]>]   
              [-e <end-row>] [-f <int>] [-fm <className>] [-np] [-r <row>] [-s   
              <comma-separated-authorizations>] [-st] [-t <table>]   
    description: scans the table, and displays the resulting records   
      -?,-help  display this help   
      -b,-begin-row <start-row>  begin row (inclusive)   
      -c,-columns   
              «columnfamily>[:<columnqualifier>],<columnfamily>[:<columnqualifier>]>   
              comma-separated columns   
      -e,-end-row <end-row>  end row (inclusive)   
      -f,-show few <int>  Only shows certain amount of characters   
      -fm,-formatter <className>  fully qualified name of the formatter class to use   
      -np,-no-pagination  disables pagination of output   
      -r,-row <row>  row to scan   
      -s,-scan-authorizations <comma-separated-authorizations>  scan authorizations   
              (all user auths are used if this argument is not specified)   
      -st,-show-timestamps  enables displaying timestamps   
      -t,-tableName <table>  table to be scanned   
  
**select**   
  
    usage: select <row> <columnfamily> <columnqualifier> [-?] [-np] [-s   
              <comma-separated-authorizations>] [-st] [-t <table>]   
    description: scans for and displays a single record   
      -?,-help  display this help   
      -np,-no-pagination  disables pagination of output   
      -s,-scan-authorizations <comma-separated-authorizations>  scan authorizations   
      -st,-show-timestamps  enables displaying timestamps   
      -t,-tableName <table>  table   
  
**selectrow**   
  
    usage: selectrow <row> [-?] [-np] [-s <comma-separated-authorizations>] [-st] [-t   
              <table>]   
    description: scans a single row and displays all resulting records   
      -?,-help  display this help   
      -np,-no-pagination  disables pagination of output   
      -s,-scan-authorizations <comma-separated-authorizations>  scan authorizations   
      -st,-show-timestamps  enables displaying timestamps   
      -t,-tableName <table>  table to row select   
  
**setauths**   
  
    usage: setauths [-?] -c | -s <comma-separated-authorizations>  [-u <user>]   
    description: sets the maximum scan authorizations for a user   
      -?,-help  display this help   
      -c,-clear-authorizations  clears the scan authorizations   
      -s,-scan-authorizations <comma-separated-authorizations>  set the scan   
              authorizations   
      -u,-user <user>  user to operate on   
  
**setgroups**   
  
    usage: setgroups <group>=<col fam>,<col fam> <group>=<col fam>,<col fam> [-?]   
              [-t <table>]   
    description: sets the locality groups for a given table (for binary or commas, use   
              Java API)   
      -?,-help  display this help   
      -t,-table <table>  get locality groups for specified table   
  
**setiter**   
  
    usage: setiter [-?] -ageoff | -agg | -class <name> | -regex | -reqvis | -vers   
              [-majc] [-minc] [-n <itername>] -p <pri>  [-scan] [-t <table>]   
    description: sets a table-specific iterator   
      -?,-help  display this help   
      -ageoff,-ageoff  an aging off type   
      -agg,-aggregator  an aggregating type   
      -class,-class-name <name>  a java class type   
      -majc,-major-compaction  applied at major compaction   
      -minc,-minor-compaction  applied at minor compaction   
      -n,-name <itername>  iterator to set   
      -p,-priority <pri>  the order in which the iterator is applied   
      -regex,-regular-expression  a regex matching type   
      -reqvis,-require-visibility  a type that omits entries with empty visibilities   
      -scan,-scan-time  applied at scan time   
      -t,-table <table>  tableName   
      -vers,-version  a versioning type   
  
**setscaniter**   
  
    usage: setscaniter [-?] -ageoff | -agg | -class <name> | -regex | -reqvis | -vers   
              [-n <itername>] -p <pri>  [-t <table>]   
    description: sets a table-specific scan iterator for this shell session   
      -?,-help  display this help   
      -ageoff,-ageoff  an aging off type   
      -agg,-aggregator  an aggregating type   
      -class,-class-name <name>  a java class type   
      -n,-name <itername>  iterator to set   
      -p,-priority <pri>  the order in which the iterator is applied   
      -regex,-regular-expression  a regex matching type   
      -reqvis,-require-visibility  a type that omits entries with empty visibilities   
      -t,-table <table>  tableName   
      -vers,-version  a versioning type   
  
**sleep**   
  
    usage: sleep [-?]   
    description: sleep for the given number of seconds   
      -?,-help  display this help   
  
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

** Up:** [Apache Accumulo User Manual Version 1.4][3] ** Previous:** [Administration][5]   ** [Contents][7]**

[3]: accumulo_user_manual.html
[5]: Administration.html
[7]: Contents.html

