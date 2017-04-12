// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

== Summary Statistics

=== Overview

Accumulo has the ability to generate summary statistics about data in a table
using user defined functions.  Currently these statistics are only generated for
data written to files.  Data recently written to Accumulo that is still in
memory will not contribute to summary statistics.

This feature can be used to inform a user about what data is in their table.
Summary statistics can also be used by compaction strategies to make decisions
about which files to compact.  

Summary data is stored in each file Accumulo produces.  Accumulo can gather
summary information from across a cluster merging it along the way.  In order
for this to be fast the, summary information should fit in cache.  There is a
dedicated cache for summary data on each tserver with a configurable size.  In
order for summary data to fit in cache, it should probably be small.

For information on writing a custom summarizer see the javadoc for
+org.apache.accumulo.core.client.summary.Summarizer+.  The package
+org.apache.accumulo.core.client.summary.summarizers+ contains summarizer
implementations that ship with Accumulo and can be configured for use.

=== Inaccuracies

Summary data can be inaccurate when files are missing summary data or when
files have extra summary data. Files can contain data outside of a tablets
boundaries. This can happen as result of bulk imported files and tablet splits.
When this happens, those files could contain extra summary information.
Accumulo offsets this some by storing summary information for multiple row
ranges per a file.  However, the ranges are not granular enough to completely
offset extra data.

Any source of inaccuracies is reported when summary information is requested.
In the shell examples below this can be seen on the +File Statistics+ line.
For files missing summary information, the compact command in the shell has a
+--sf-no-summary+ option.  This options compacts files that do not have the
summary information configured for the table.  The compact command also has the
+--sf-extra-summary+ option which will compact files with extra summary
information.

=== Configuring

The following tablet server and table properties configure summarization.

* <<appendices/config.txt#_tserver_cache_summary_size>>
* <<appendices/config.txt#_tserver_summary_partition_threads>>
* <<appendices/config.txt#_tserver_summary_remote_threads>>
* <<appendices/config.txt#_tserver_summary_retrieval_threads>>
* <<appendices/config.txt#TABLE_SUMMARIZER_PREFIX>>
* <<appendices/config.txt#_table_file_summary_maxsize>>

=== Permissions

Because summary data may be derived from sensitive data, requesting summary data
requires a special permission.  User must have the table permission
+GET_SUMMARIES+ in order to retrieve summary data.


=== Bulk import

When generating rfiles to bulk import into Accumulo, those rfiles can contain
summary data.  To use this feature, look at the javadoc on the
+AccumuloFileOutputFormat.setSummarizers(...)+ method.  Also,
+org.apache.accumulo.core.client.rfile.RFile+ has options for creating RFiles
with embedded summary data.

=== Examples

This example walks through using summarizers in the Accumulo shell.  Below a
table is created and some data is inserted to summarize.

 root@uno> createtable summary_test
 root@uno summary_test> setauths -u root -s PI,GEO,TIME
 root@uno summary_test> insert 3b503bd name last Doe
 root@uno summary_test> insert 3b503bd name first John
 root@uno summary_test> insert 3b503bd contact address "123 Park Ave, NY, NY" -l PI&GEO
 root@uno summary_test> insert 3b503bd date birth "1/11/1942" -l PI&TIME
 root@uno summary_test> insert 3b503bd date married "5/11/1962" -l PI&TIME
 root@uno summary_test> insert 3b503bd contact home_phone 1-123-456-7890 -l PI
 root@uno summary_test> insert d5d18dd contact address "50 Lake Shore Dr, Chicago, IL" -l PI&GEO
 root@uno summary_test> insert d5d18dd name first Jane
 root@uno summary_test> insert d5d18dd name last Doe
 root@uno summary_test> insert d5d18dd date birth 8/15/1969 -l PI&TIME
 root@uno summary_test> scan -s PI,GEO,TIME
 3b503bd contact:address [PI&GEO]    123 Park Ave, NY, NY
 3b503bd contact:home_phone [PI]    1-123-456-7890
 3b503bd date:birth [PI&TIME]    1/11/1942
 3b503bd date:married [PI&TIME]    5/11/1962
 3b503bd name:first []    John
 3b503bd name:last []    Doe
 d5d18dd contact:address [PI&GEO]    50 Lake Shore Dr, Chicago, IL
 d5d18dd date:birth [PI&TIME]    8/15/1969
 d5d18dd name:first []    Jane
 d5d18dd name:last []    Doe

After inserting the data, summaries are requested below.  No summaries are returned.

 root@uno summary_test> summaries

The visibility summarizer is configured below and the table is flushed.
Flushing the table creates a file creating summary data in the process. The
summary data returned counts how many times each column visibility occurred.
The statistics with a +c:+ prefix are visibilities.  The others are generic
statistics created by the CountingSummarizer that VisibilitySummarizer extends. 

 root@uno summary_test> config -t summary_test -s table.summarizer.vis=org.apache.accumulo.core.client.summary.summarizers.VisibilitySummarizer
 root@uno summary_test> summaries
 root@uno summary_test> flush -w
 2017-02-24 19:54:46,090 [shell.Shell] INFO : Flush of table summary_test completed.
 root@uno summary_test> summaries
  Summarizer         : org.apache.accumulo.core.client.summary.summarizers.VisibilitySummarizer vis {}
  File Statistics    : [total:1, missing:0, extra:0, large:0]
  Summary Statistics : 
     c:                                                           = 4
     c:PI                                                         = 1
     c:PI&GEO                                                     = 2
     c:PI&TIME                                                    = 3
     emitted                                                      = 10
     seen                                                         = 10
     tooLong                                                      = 0
     tooMany                                                      = 0

VisibilitySummarizer has an option +maxCounters+ that determines the max number
of column visibilites it will track.  Below this option is set and compaction
is forced to regenerate summary data.  The new summary data only has three
visibilites and now the +tooMany+ statistic is 4.  This is the number of
visibilites that were not counted.

 root@uno summary_test> config -t summary_test -s table.summarizer.vis.opt.maxCounters=3
 root@uno summary_test> compact -w
 2017-02-24 19:54:46,267 [shell.Shell] INFO : Compacting table ...
 2017-02-24 19:54:47,127 [shell.Shell] INFO : Compaction of table summary_test completed for given range
 root@uno summary_test> summaries
  Summarizer         : org.apache.accumulo.core.client.summary.summarizers.VisibilitySummarizer vis {maxCounters=3}
  File Statistics    : [total:1, missing:0, extra:0, large:0]
  Summary Statistics : 
     c:PI                                                         = 1
     c:PI&GEO                                                     = 2
     c:PI&TIME                                                    = 3
     emitted                                                      = 10
     seen                                                         = 10
     tooLong                                                      = 0
     tooMany                                                      = 4

Another summarizer is configured below that tracks the number of deletes.  Also
a compaction strategy that uses this summary data is configured.  The
+TooManyDeletesCompactionStrategy+ will force a compaction of the tablet when
the ratio of deletes to non-deletes is over 25%.  This threshold is
configurable.  Below a delete is added and its reflected in the statistics.  In
this case there is 1 delete and 10 non-deletes, not enough to force a
compaction of the tablet.

....
root@uno summary_test> config -t summary_test -s table.summarizer.del=org.apache.accumulo.core.client.summary.summarizers.DeletesSummarizer
root@uno summary_test> compact -w
2017-02-24 19:54:47,282 [shell.Shell] INFO : Compacting table ...
2017-02-24 19:54:49,236 [shell.Shell] INFO : Compaction of table summary_test completed for given range
root@uno summary_test> config -t summary_test -s table.compaction.major.ratio=10
root@uno summary_test> config -t summary_test -s table.majc.compaction.strategy=org.apache.accumulo.tserver.compaction.strategies.TooManyDeletesCompactionStrategy
root@uno summary_test> deletemany -r d5d18dd -c date -f
[DELETED] d5d18dd date:birth [PI&TIME]
root@uno summary_test> flush -w
2017-02-24 19:54:49,686 [shell.Shell] INFO : Flush of table summary_test completed.
root@uno summary_test> summaries
 Summarizer         : org.apache.accumulo.core.client.summary.summarizers.VisibilitySummarizer vis {maxCounters=3}
 File Statistics    : [total:2, missing:0, extra:0, large:0]
 Summary Statistics : 
    c:PI                                                         = 1
    c:PI&GEO                                                     = 2
    c:PI&TIME                                                    = 4
    emitted                                                      = 11
    seen                                                         = 11
    tooLong                                                      = 0
    tooMany                                                      = 4

 Summarizer         : org.apache.accumulo.core.client.summary.summarizers.DeletesSummarizer del {}
 File Statistics    : [total:2, missing:0, extra:0, large:0]
 Summary Statistics : 
    deletes                                                      = 1
    total                                                        = 11
....

Some more deletes are added and the table is flushed below.  This results in 4
deletes and 10 non-deletes, which triggers a full compaction.  A full
compaction of all files is the only time when delete markers are dropped.  The
compaction ratio was set to 10 above to show that the number of files did not
trigger the compaction.   After the compaction there no deletes 6 non-deletes.

....
root@uno summary_test> deletemany -r d5d18dd -f
[DELETED] d5d18dd contact:address [PI&GEO]
[DELETED] d5d18dd name:first []
[DELETED] d5d18dd name:last []
root@uno summary_test> flush -w
2017-02-24 19:54:52,800 [shell.Shell] INFO : Flush of table summary_test completed.
root@uno summary_test> summaries
 Summarizer         : org.apache.accumulo.core.client.summary.summarizers.VisibilitySummarizer vis {maxCounters=3}
 File Statistics    : [total:1, missing:0, extra:0, large:0]
 Summary Statistics : 
    c:PI                                                         = 1
    c:PI&GEO                                                     = 1
    c:PI&TIME                                                    = 2
    emitted                                                      = 6
    seen                                                         = 6
    tooLong                                                      = 0
    tooMany                                                      = 2

 Summarizer         : org.apache.accumulo.core.client.summary.summarizers.DeletesSummarizer del {}
 File Statistics    : [total:1, missing:0, extra:0, large:0]
 Summary Statistics : 
    deletes                                                      = 0
    total                                                        = 6
root@uno summary_test>   
....

