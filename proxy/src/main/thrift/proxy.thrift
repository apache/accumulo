/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

/*
 * Compatibility.
 *
 * Its possible that over time that this IDL may change in ways that breaks
 * client code.  However newer versions of the proxy server will use thrift
 * mechanisms to maintain compatibility with older versions of the proxy IDL.
 * So proxy clients using 1.4.x or 1.5.x IDL can use a 1.6.x proxy server.
 * Therefore if changes to this IDL break your client code, then using an older
 * version of the IDL with a new version of the proxy server is an option.
 */

namespace cpp accumulo
namespace java org.apache.accumulo.proxy.thrift
namespace rb Accumulo
namespace py accumulo

struct Key {
  1:binary row;
  2:binary colFamily;
  3:binary colQualifier;
  4:binary colVisibility;
  5:optional i64 timestamp = 0x7FFFFFFFFFFFFFFF
}

enum PartialKey {
  ROW,
  ROW_COLFAM,
  ROW_COLFAM_COLQUAL,
  ROW_COLFAM_COLQUAL_COLVIS,
  ROW_COLFAM_COLQUAL_COLVIS_TIME,
  ROW_COLFAM_COLQUAL_COLVIS_TIME_DEL
}

struct ColumnUpdate {
  1:binary colFamily,
  2:binary colQualifier,
  3:optional binary colVisibility,
  4:optional i64 timestamp,
  5:optional binary value,
  6:optional bool deleteCell
}

//since 1.6.0
struct DiskUsage {
  1:list<string> tables,
  2:i64 usage
}

struct KeyValue {
  1:Key key,
  2:binary value
}

struct ScanResult {
  1:list<KeyValue> results,
  2:bool more
}

struct Range {
  1:Key start,
  2:bool startInclusive
  3:Key stop,
  4:bool stopInclusive
}

struct ScanColumn {
  1:binary colFamily,
  2:optional binary colQualifier
}

struct IteratorSetting {
  1: i32 priority,
  2: string name,
  3: string iteratorClass,
  4: map<string,string> properties
}

struct ScanOptions {
  1:optional set<binary> authorizations;
  2:optional Range range,
  3:optional list<ScanColumn> columns;
  4:optional list<IteratorSetting> iterators;
  5:optional i32 bufferSize;
} 

struct BatchScanOptions {
  1:optional set<binary> authorizations;
  2:optional list<Range> ranges;
  3:optional list<ScanColumn> columns;
  4:optional list<IteratorSetting> iterators;
  5:optional i32 threads;
} 

struct KeyValueAndPeek {
  1:KeyValue keyValue,
  2:bool hasNext
}

enum TablePermission {
  READ = 2,
  WRITE = 3,
  BULK_IMPORT = 4,
  ALTER_TABLE = 5,
  GRANT = 6,
  DROP_TABLE = 7,
}

enum SystemPermission {
  GRANT = 0,
  CREATE_TABLE = 1,
  DROP_TABLE = 2,
  ALTER_TABLE = 3,
  CREATE_USER = 4,
  DROP_USER = 5,
  ALTER_USER = 6,
  SYSTEM = 7,
  CREATE_NAMESPACE = 8,
  DROP_NAMESPACE = 9,
  ALTER_NAMESPACE = 10,
  OBTAIN_DELEGATION_TOKEN = 11,
}

enum NamespacePermission {
  READ = 0,
  WRITE = 1,
  ALTER_NAMESPACE = 2,
  GRANT = 3,
  ALTER_TABLE = 4,
  CREATE_TABLE = 5,
  DROP_TABLE = 6,
  BULK_IMPORT = 7,
  DROP_NAMESPACE = 8
}

enum ScanType {
    SINGLE,
    BATCH
}

enum ScanState {
    IDLE,
    RUNNING,
    QUEUED
}

struct KeyExtent {
  1:string tableId,
  2:binary endRow,
  3:binary prevEndRow
}

struct Column {
  1:binary colFamily;
  2:binary colQualifier;
  3:binary colVisibility;
}

//since 1.6.0
struct Condition {
  1:Column column;
  2:optional i64 timestamp;
  3:optional binary value;
  4:optional list<IteratorSetting> iterators;
}

//since 1.6.0
struct ConditionalUpdates {
	2:list<Condition> conditions
	3:list<ColumnUpdate> updates 
}

//since 1.6.0
enum ConditionalStatus {
  ACCEPTED,
  REJECTED,
  VIOLATED,
  UNKNOWN,
  INVISIBLE_VISIBILITY
}

//since 1.7.0
enum Durability {
	DEFAULT,
	NONE,
	LOG,
	FLUSH,
	SYNC
}

//since 1.6.0
struct ConditionalWriterOptions {
   1:optional i64 maxMemory
   2:optional i64 timeoutMs
   3:optional i32 threads
   4:optional set<binary> authorizations;
   //since 1.7.0
   5:optional Durability durability;
}

struct ActiveScan {
  1:string client
  2:string user
  3:string table
  4:i64 age
  5:i64 idleTime
  6:ScanType type
  7:ScanState state
  8:KeyExtent extent
  9:list<Column> columns
  10:list<IteratorSetting> iterators
  11:list<binary> authorizations
}

enum CompactionType {
   MINOR,
   MERGE,
   MAJOR,
   FULL
}

enum CompactionReason {
   USER,
   SYSTEM,
   CHOP,
   IDLE,
   CLOSE
}

struct ActiveCompaction {
  1:KeyExtent extent
  2:i64 age
  3:list<string> inputFiles
  4:string outputFile
  5:CompactionType type
  6:CompactionReason reason
  7:string localityGroup
  8:i64 entriesRead
  9:i64 entriesWritten
  10:list<IteratorSetting> iterators;
}

struct WriterOptions {
 1:i64 maxMemory
 2:i64 latencyMs
 3:i64 timeoutMs
 4:i32 threads
 //since 1.7.0
 5:optional Durability durability
}

struct CompactionStrategyConfig {
  1:string className
  2:map<string,string> options
}

enum IteratorScope {
  MINC,
  MAJC,
  SCAN
}

enum TimeType {
  LOGICAL,
  MILLIS
}

exception UnknownScanner {
  1:string msg
}

exception UnknownWriter {
  1:string msg
}

exception NoMoreEntriesException {
  1:string msg
}

exception AccumuloException {
  1:string msg
}

exception AccumuloSecurityException {
  1:string msg
}

exception TableNotFoundException {
  1:string msg
}

exception TableExistsException {
  1:string msg
}

exception MutationsRejectedException {
  1:string msg
}

exception NamespaceExistsException {
  1:string msg
}

exception NamespaceNotFoundException {
  1:string msg
}

exception NamespaceNotEmptyException {
  1:string msg
}

service AccumuloProxy
{
  // get an authentication token
  binary login(1:string principal, 2:map<string, string> loginProperties)                              throws (1:AccumuloSecurityException ouch2);

  // table operations
  i32 addConstraint (1:binary login, 2:string tableName, 3:string constraintClassName)                 throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);
  void addSplits (1:binary login, 2:string tableName, 3:set<binary> splits)                            throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);
  void attachIterator (1:binary login, 2:string tableName, 3:IteratorSetting setting, 
                       4:set<IteratorScope> scopes) 
                                                                                                       throws (1:AccumuloSecurityException ouch1, 2:AccumuloException ouch2, 3:TableNotFoundException ouch3);
  void checkIteratorConflicts (1:binary login, 2:string tableName, 3:IteratorSetting setting, 
                               4:set<IteratorScope> scopes) 
                                                                                                       throws (1:AccumuloSecurityException ouch1, 2:AccumuloException ouch2, 3:TableNotFoundException ouch3);
  void clearLocatorCache (1:binary login, 2:string tableName)                                          throws (1:TableNotFoundException ouch1);
  void cloneTable (1:binary login, 2:string tableName, 3:string newTableName, 4:bool flush, 
                   5:map<string,string> propertiesToSet, 6:set<string> propertiesToExclude) 
                                                                                                       throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3, 4:TableExistsException ouch4);
  //changed in 1.7.0, see comment at top about compatibility
  void compactTable (1:binary login, 2:string tableName, 3:binary startRow, 4:binary endRow, 
		     5:list<IteratorSetting> iterators, 6:bool flush, 7:bool wait, 
		     8:CompactionStrategyConfig compactionStrategy)                                            throws (1:AccumuloSecurityException ouch1, 2:TableNotFoundException ouch2, 3:AccumuloException ouch3);
  void cancelCompaction(1:binary login, 2:string tableName)                                            throws (1:AccumuloSecurityException ouch1, 2:TableNotFoundException ouch2, 3:AccumuloException ouch3);
                                                                                                            
  void createTable (1:binary login, 2:string tableName, 3:bool versioningIter, 4:TimeType type)        throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableExistsException ouch3);
  void deleteTable (1:binary login, 2:string tableName)                                                throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);
  void deleteRows (1:binary login, 2:string tableName, 3:binary startRow, 4:binary endRow)             throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);
  void exportTable (1:binary login, 2:string tableName, 3:string exportDir)                            throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);
  void flushTable (1:binary login, 2:string tableName, 3:binary startRow, 4:binary endRow, 
                   5:bool wait)
                                                                                                       throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);
  //since 1.6.0                                                                                                       
  list<DiskUsage> getDiskUsage(1:binary login, 2:set<string> tables)                                   throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);
  map<string,set<string>> getLocalityGroups (1:binary login, 2:string tableName)                       throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);
  IteratorSetting getIteratorSetting (1:binary login, 2:string tableName, 
                                      3:string iteratorName, 4:IteratorScope scope) 
                                                                                                       throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);
  binary getMaxRow (1:binary login, 2:string tableName, 3:set<binary> auths, 4:binary startRow, 
                    5:bool startInclusive, 6:binary endRow, 7:bool endInclusive) 
                                                                                                       throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);
  map<string,string> getTableProperties (1:binary login, 2:string tableName)                           throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);
  void importDirectory (1:binary login, 2:string tableName, 3:string importDir, 
                        4:string failureDir, 5:bool setTime) 
                                                                                                       throws (1:TableNotFoundException ouch1, 2:AccumuloException ouch3, 3:AccumuloSecurityException ouch4);
  void importTable (1:binary login, 2:string tableName, 3:string importDir)                            throws (1:TableExistsException ouch1, 2:AccumuloException ouch2, 3:AccumuloSecurityException ouch3);
  list<binary> listSplits (1:binary login, 2:string tableName, 3:i32 maxSplits)                        throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);
  set<string> listTables (1:binary login);
  map<string,set<IteratorScope>> listIterators (1:binary login, 2:string tableName)                    throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);
  map<string,i32> listConstraints (1:binary login, 2:string tableName)                                 throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);
  void mergeTablets (1:binary login, 2:string tableName, 3:binary startRow, 4:binary endRow)           throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);
  //changed in 1.6.0, see comment at top about compatibility
  void offlineTable (1:binary login, 2:string tableName, 3:bool wait=false)                            throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);
  //changed in 1.6.0, see comment at top about compatibility
  void onlineTable (1:binary login, 2:string tableName, 3:bool wait=false)                             throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);
  void removeConstraint (1:binary login, 2:string tableName, 3:i32 constraint)                         throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);
  void removeIterator (1:binary login, 2:string tableName, 3:string iterName, 
                       4:set<IteratorScope> scopes)
                                                                                                       throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);
  void removeTableProperty (1:binary login, 2:string tableName, 3:string property)                     throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);
  void renameTable (1:binary login, 2:string oldTableName, 3:string newTableName)                      throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3, 4:TableExistsException ouch4);
  void setLocalityGroups (1:binary login, 2:string tableName, 3:map<string,set<string>> groups)        throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);
  void setTableProperty (1:binary login, 2:string tableName, 3:string property, 4:string value)        throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);
  set<Range> splitRangeByTablets (1:binary login, 2:string tableName, 3:Range range, 4:i32 maxSplits)  throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);
  bool tableExists (1:binary login, 2:string tableName);
  map<string,string> tableIdMap (1:binary login);
  bool testTableClassLoad (1:binary login, 2:string tableName, 3:string className                     
                           , 4:string asTypeName)                                                      throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);

  // instance operations
  void pingTabletServer(1:binary login, 2:string tserver)                                            throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  list<ActiveScan> getActiveScans (1:binary login, 2:string tserver)                                 throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  list<ActiveCompaction> getActiveCompactions(1:binary login, 2:string tserver)                      throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  map<string,string> getSiteConfiguration (1:binary login)                                           throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  map<string,string> getSystemConfiguration (1:binary login)                                         throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  list<string> getTabletServers (1:binary login);
  void removeProperty (1:binary login, 2:string property)                                            throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  void setProperty (1:binary login, 2:string property, 3:string value)                               throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  bool testClassLoad (1:binary login, 2:string className, 3:string asTypeName)                       throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);

  // security operations
  bool authenticateUser (1:binary login, 2:string user, 3:map<string, string> properties)            throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  void changeUserAuthorizations (1:binary login, 2:string user, 3:set<binary> authorizations)        throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  void changeLocalUserPassword (1:binary login, 2:string user, 3:binary password)                    throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  void createLocalUser (1:binary login, 2:string user, 3:binary password)                            throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  void dropLocalUser (1:binary login, 2:string user)                                                 throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  list<binary> getUserAuthorizations (1:binary login, 2:string user)                                 throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  void grantSystemPermission (1:binary login, 2:string user, 3:SystemPermission perm)                throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  void grantTablePermission (1:binary login, 2:string user, 3:string table, 4:TablePermission perm)  throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);
  bool hasSystemPermission (1:binary login, 2:string user, 3:SystemPermission perm)                  throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  bool hasTablePermission (1:binary login, 2:string user, 3:string table, 4:TablePermission perm)    throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);
  set<string> listLocalUsers (1:binary login)                                                        throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);
  void revokeSystemPermission (1:binary login, 2:string user, 3:SystemPermission perm)               throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  void revokeTablePermission (1:binary login, 2:string user, 3:string table, 4:TablePermission perm) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);
  void grantNamespacePermission (1:binary login, 2:string user, 3:string namespaceName,
                                 4:NamespacePermission perm)                                         throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  bool hasNamespacePermission (1:binary login, 2:string user, 3:string namespaceName,
                               4:NamespacePermission perm)                                           throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  void revokeNamespacePermission (1:binary login, 2:string user, 3:string namespaceName,
                                  4:NamespacePermission perm)                                        throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);

  // scanning
  string createBatchScanner(1:binary login, 2:string tableName, 3:BatchScanOptions options)          throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);
  string createScanner(1:binary login, 2:string tableName, 3:ScanOptions options)                    throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);

  // use the scanner
  bool hasNext(1:string scanner)                        throws(1:UnknownScanner ouch1);
  KeyValueAndPeek nextEntry(1:string scanner)           throws(1:NoMoreEntriesException ouch1, 2:UnknownScanner ouch2, 3:AccumuloSecurityException ouch3);
  ScanResult nextK(1:string scanner, 2:i32 k)           throws(1:NoMoreEntriesException ouch1, 2:UnknownScanner ouch2, 3:AccumuloSecurityException ouch3);
  void closeScanner(1:string scanner)                   throws(1:UnknownScanner ouch1);

  // writing
  void updateAndFlush(1:binary login, 2:string tableName, 3:map<binary, list<ColumnUpdate>> cells) throws(1:AccumuloException outch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3, 4:MutationsRejectedException ouch4);
  string createWriter(1:binary login, 2:string tableName, 3:WriterOptions opts)                    throws(1:AccumuloException outch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);

  // use the writer
  oneway void update(1:string writer, 2:map<binary, list<ColumnUpdate>> cells);
  void flush(1:string writer)                                                                      throws (1:UnknownWriter ouch1, 2:MutationsRejectedException ouch2);
  void closeWriter(1:string writer)                                                                throws (1:UnknownWriter ouch1, 2:MutationsRejectedException ouch2);

  //api for a single conditional update
  ConditionalStatus updateRowConditionally(1:binary login, 2:string tableName, 3:binary row, 
                                           4:ConditionalUpdates updates)                           throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);
  
  //api for batch conditional updates
  //since 1.6.0
  string createConditionalWriter(1:binary login, 2:string tableName, 
                                 3:ConditionalWriterOptions options)                               throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);
  //since 1.6.0
  map<binary, ConditionalStatus> updateRowsConditionally(1:string conditionalWriter,                   
                                                     2:map<binary, ConditionalUpdates> updates)    throws (1:UnknownWriter ouch1, 2:AccumuloException ouch2, 3:AccumuloSecurityException ouch3);
  //since 1.6.0
  void closeConditionalWriter(1:string conditionalWriter);

  // utilities
  Range getRowRange(1:binary row);
  Key getFollowing(1:Key key, 2:PartialKey part);

  // namespace operations, since 1.8.0
  string systemNamespace();
  string defaultNamespace();
  list<string> listNamespaces(1:binary login)                                                      throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  bool namespaceExists(1:binary login, 2:string namespaceName)                                     throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  void createNamespace(1:binary login, 2:string namespaceName)                                     throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:NamespaceExistsException ouch3);
  void deleteNamespace(1:binary login, 2:string namespaceName)                                     throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:NamespaceNotFoundException ouch3, 4:NamespaceNotEmptyException ouch4);
  void renameNamespace(1:binary login, 2:string oldNamespaceName, 3:string newNamespaceName)       throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:NamespaceNotFoundException ouch3, 4:NamespaceExistsException ouch4);
  void setNamespaceProperty(1:binary login, 2:string namespaceName, 3:string property,
                            4:string value)                                                        throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:NamespaceNotFoundException ouch3);
  void removeNamespaceProperty(1:binary login, 2:string namespaceName, 3:string property)          throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:NamespaceNotFoundException ouch3);
  map<string,string> getNamespaceProperties(1:binary login, 2:string namespaceName)                throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:NamespaceNotFoundException ouch3);
  map<string,string> namespaceIdMap(1:binary login)                                                throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  void attachNamespaceIterator(1:binary login, 2:string namespaceName, 3:IteratorSetting setting,
                               4:set<IteratorScope> scopes)                                        throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:NamespaceNotFoundException ouch3);
  void removeNamespaceIterator(1:binary login, 2:string namespaceName, 3:string name,
                               4:set<IteratorScope> scopes)                                        throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:NamespaceNotFoundException ouch3);
  IteratorSetting getNamespaceIteratorSetting(1:binary login, 2:string namespaceName,
                                              3:string name, 4:IteratorScope scope)                throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:NamespaceNotFoundException ouch3);
  map<string,set<IteratorScope>> listNamespaceIterators(1:binary login, 2:string namespaceName)    throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:NamespaceNotFoundException ouch3);
  void checkNamespaceIteratorConflicts(1:binary login, 2:string namespaceName,
                                       3:IteratorSetting setting, 4:set<IteratorScope> scopes)     throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:NamespaceNotFoundException ouch3);
  i32 addNamespaceConstraint(1:binary login, 2:string namespaceName,
                             3:string constraintClassName)                                         throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:NamespaceNotFoundException ouch3);
  void removeNamespaceConstraint(1:binary login, 2:string namespaceName, 3:i32 id)                 throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:NamespaceNotFoundException ouch3);
  map<string,i32> listNamespaceConstraints(1:binary login, 2:string namespaceName)                 throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:NamespaceNotFoundException ouch3);
  bool testNamespaceClassLoad(1:binary login, 2:string namespaceName, 3:string className,
                              4:string asTypeName)                                                 throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:NamespaceNotFoundException ouch3);
}
