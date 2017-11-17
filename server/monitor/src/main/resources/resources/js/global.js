/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the 'License'); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an 'AS IS' BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

/**
 * Descriptions used for table columns
 */
var descriptions = {
  '# Tablets' : 'Tables are broken down into ranges of rows called tablets.',
  '# Offline Tablets' : 'Tablets unavailable for query or ingest. May be a' +
      ' transient condition when tablets are moved for balancing.',
  'Entries' : 'Key/value pairs over each instance, table or tablet.',
  'Entries in Memory' : 'The total number of key/value pairs stored in' +
      ' memory and not yet written to disk.',
  'Ingest' : 'The number of Key/Value pairs inserted. (Note that deletes' +
      ' are "inserted")',
  'Response Time' : 'The time it took for the tserver to return its status.',
  'Entries Read' : 'The number of Key/Value pairs read on the server side.' +
      'Not all key values read may be returned to client because of filtering.',
  'Entries Returned' : 'The number of Key/Value pairs returned to clients' +
      'during queries. This is not the number of scans.',
  'Hold Time' : 'The amount of time that ingest operations are suspended' +
      ' while waiting for data to be written to disk.',
  'Running Scans' : 'Information about the scans threads. Shows how many' +
      ' threads are running and how much work is queued for the threads.',
  'Minor Compactions' : 'Flushing memory to disk is called a "Minor' +
      ' Compaction." Multiple tablets can be minor compacted simultaneously,' +
      ' but sometimes they must wait for resources to be available.' +
      ' These tablets that are waiting for compaction are "queued"' +
      ' and are indicated using parentheses. So 2 (3) indicates there' +
      ' are two compactions running and three queued waiting for resources.',
  'Major Compactions' : 'Gathering up many small files and rewriting them' +
      ' as one larger file is called a "Major Compaction". Major Compactions' +
      ' are performed as a consequence of new files created from Minor' +
      ' Compactions and Bulk Load operations. They reduce the number of' +
      ' files used during queries.',
  'Master' : 'The hostname of the master server',
  '# Online Tablet Servers' : 'Number of tablet servers currently available',
  '# Total Tablet Servers' : 'The total number of tablet servers configured',
  'Last GC' : 'The last time files were cleaned-up from HDFS.',
  'Total Entries' : 'The total number of key/value pairs in Accumulo',
  'Total Ingest' : 'The number of Key/Value pairs inserted, per second.' +
      ' Note that deleted records are "inserted" and will make the ingest' +
      ' rate increase in the near-term.',
  'Total Entries Read' : 'The total number of Key/Value pairs read on the' +
      ' server side.  Not all may be returned because of filtering.',
  'Total Entries Returned' : 'The total number of Key/Value pairs returned' +
      ' as a result of scans.',
  'Max Hold Time' : 'The maximum amount of time that ingest has been held' +
      ' across all servers due to a lack of memory to store the records',
  'OS Load' : 'The Unix one minute load average. The average number of' +
      ' processes in the run queue over a one minute interval.',
  'Query' : 'The number of key/value pairs returned to clients.' +
      ' (Not the number of scans)',
  'Index Cache Hit Rate' : 'The recent index cache hit rate.',
  'Data Cache Hit Rate' : 'The recent data cache hit rate.',
  '# Scans' : 'Number of scans presently running',
  'Oldest Scan' : 'The age of the oldest scan on this server.',
  'Import Age' : 'The age of the import.',
  'Import State' : 'The current state of the bulk import',
  '# Imports' : 'Number of imports presently running',
  'Oldest Age' : 'The age of the oldest import running on this server.',
  'Trace Start' : 'Start Time of selected trace type',
  'Span Time' : 'Span Time of selected trace type',
  'Source' : 'Service and Location of selected trace type',
  'Trace Type' : 'Trace Type',
  'Total Spans' : 'Number of spans of this type',
  'Short Span' : 'Shortest span duration',
  'Long Span' : 'Longest span duration',
  'Avg Span' : 'Average span duration',
  'Histogram' : 'Counts of spans of different duration. Columns start' +
      ' at milliseconds, and each column is ten times longer: tens of' +
      ' milliseconds, seconds, tens of seconds, etc.'
};

/**
 * List of namespaces in Accumulo
 */
var NAMESPACES = '';

/**
 * Timer object
 */
var TIMER;
