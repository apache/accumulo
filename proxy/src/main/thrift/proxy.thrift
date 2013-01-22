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
namespace java org.apache.accumulo.proxy.thrift

struct PKey {
	1:binary row;
	2:binary colFamily;
	3:binary colQualifier;
	4:binary colVisibility;
	5:optional i64 timestamp
}

enum PPartialKey {
  ROW,
  ROW_COLFAM,
  ROW_COLFAM_COLQUAL,
  ROW_COLFAM_COLQUAL_COLVIS,
  ROW_COLFAM_COLQUAL_COLVIS_TIME,
  ROW_COLFAM_COLQUAL_COLVIS_TIME_DEL
}

struct PColumnUpdate {
	1:binary colFamily;
	2:binary colQualifier;
	3:optional binary colVisibility;
	4:optional i64 timestamp;
	5:optional binary value;
	6:optional bool deleteCell;
}

struct PKeyValue {
	1:PKey key,
	2:binary value
}

struct PScanResult {
	1:list<PKeyValue> results,
	2:bool more
}

struct PRange {
        1:PKey start,
	2:bool startInclusive
        3:PKey stop,
	4:bool stopInclusive
}

struct UserPass
{
  1:string username;
  2:binary password;
}

struct PIteratorSetting {
  1: i32 priority;
  2: string name;
  3: string iteratorClass;
  4: map<string,string> properties;
}

struct KeyValueAndPeek{
    1:PKeyValue keyValue;
    2:bool hasNext;
}

enum PTablePermission {
  READ = 2,
  WRITE = 3,
  BULK_IMPORT = 4,
  ALTER_TABLE = 5,
  GRANT = 6,
  DROP_TABLE = 7,
}

enum PSystemPermission {
  GRANT = 0,
  CREATE_TABLE = 1,
  DROP_TABLE = 2,
  ALTER_TABLE = 3,
  CREATE_USER = 4,
  DROP_USER = 5,
  ALTER_USER = 6,
  SYSTEM = 7,
}

enum PScanType {
    SINGLE,
    BATCH
}

enum PScanState {
    IDLE,
    RUNNING,
    QUEUED
}

struct PKeyExtent {
  1:binary tableId,
  2:binary endRow,
  3:binary prevEndRow
}

struct PColumn {
  1:binary colFamily;
  2:binary colQualifier;
  3:binary colVisibility;
}

struct PActiveScan {
    1:string client
    2:string user
    3:string tableId
    4:i64 age
    5:i64 idleTime
    6:PScanType type
    7:PScanState state
    8:PKeyExtent extent
    9:list<PColumn> columns
    10:list<PIteratorSetting> iterators
    11:list<binary> authorizations
}

enum PCompactionType {
   MINOR,
   MERGE,
   MAJOR,
   FULL
}

enum PCompactionReason {
   USER,
   SYSTEM,
   CHOP,
   IDLE,
   CLOSE
}

struct PActiveCompaction {
    1:PKeyExtent extent
    2:i64 age
    3:list<string> inputFiles
    4:string outputFile
    5:PCompactionType type
    6:PCompactionReason reason
    7:string localityGroup
    8:i64 entriesRead
    9:i64 entriesWritten
    10:list<PIteratorSetting> iterators;
}

enum PIteratorScope {
  MINC,
  MAJC,
  SCAN
}

exception NoMoreEntriesException
{
  1:string msg;
}

exception AccumuloException
{
  1:string msg;
}
exception AccumuloSecurityException
{
  1:string msg;
}
exception TableNotFoundException
{
  1:string msg;
}

exception TableExistsException
{
  1:string msg;
}

exception IOException
{
  1:string msg;
}

service AccumuloProxy
{
  bool ping (1:UserPass userpass);

  //table operations

  i32 tableOperations_addConstraint (1:UserPass userpass, 2:string tableName, 3:string constraintClassName) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);
  void tableOperations_addSplits (1:UserPass userpass, 2:string tableName, 3:set<binary> splits) throws (1:TableNotFoundException ouch1, 2:AccumuloException ouch2, 3:AccumuloSecurityException ouch3);
  void tableOperations_clearLocatorCache (1:UserPass userpass, 2:string tableName) throws (1:TableNotFoundException ouch1);
  void tableOperations_compact (1:UserPass userpass, 2:string tableName, 3:binary startRow, 4:binary endRow, 5:bool flush, 6:bool wait) throws (1:AccumuloSecurityException ouch1, 2:TableNotFoundException ouch2, 3:AccumuloException ouch3);
  void tableOperations_create (1:UserPass userpass, 2:string tableName) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableExistsException ouch3);
  void tableOperations_delete (1:UserPass userpass, 2:string tableName) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);
  void tableOperations_deleteRows (1:UserPass userpass, 2:string tableName, 3:binary startRow, 4:binary endRow) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);
  bool tableOperations_exists (1:UserPass userpass, 2:string tableName);
  void tableOperations_flush (1:UserPass userpass, 2:string tableName, 3:binary startRow, 4:binary endRow, 5:bool wait) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  map<string,set<string>> tableOperations_getLocalityGroups (1:UserPass userpass, 2:string tableName) throws (1:AccumuloException ouch1, 2:TableNotFoundException ouch2);
  binary tableOperations_getMaxRow (1:UserPass userpass, 2:string tableName, 3:list<binary> auths, 4:binary startRow, 5:bool startInclusive, 6:binary endRow, 7:bool endInclusive) throws (1:TableNotFoundException ouch1, 2:AccumuloException ouch2, 3:AccumuloSecurityException ouch3);
  map<string,string> tableOperations_getProperties (1:UserPass userpass, 2:string tableName) throws (1:AccumuloException ouch1, 2:TableNotFoundException ouch2);
  list<binary> tableOperations_getSplits (1:UserPass userpass, 2:string tableName, 3:i32 maxSplits) throws (1:TableNotFoundException ouch1);
  set<string> tableOperations_list (1:UserPass userpass);
  map<string,i32> tableOperations_listConstraints (1:UserPass userpass, 2:string tableName) throws (1:AccumuloException ouch1, 2:TableNotFoundException ouch2);
  void tableOperations_merge (1:UserPass userpass, 2:string tableName, 3:binary startRow, 4:binary endRow) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);
  void tableOperations_offline (1:UserPass userpass, 2:string tableName) throws (1:AccumuloSecurityException ouch1, 2:AccumuloException ouch2, 3:TableNotFoundException ouch3);
  void tableOperations_online (1:UserPass userpass, 2:string tableName) throws (1:AccumuloSecurityException ouch1, 2:AccumuloException ouch2, 3:TableNotFoundException ouch3);
  void tableOperations_removeConstraint (1:UserPass userpass, 2:string tableName, 3:i32 constraint) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  void tableOperations_removeProperty (1:UserPass userpass, 2:string tableName, 3:string property) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  void tableOperations_rename (1:UserPass userpass, 2:string oldTableName, 3:string newTableName) throws (1:AccumuloSecurityException ouch1, 2:TableNotFoundException ouch2, 3:AccumuloException ouch3, 4:TableExistsException ouch4);
  void tableOperations_setLocalityGroups (1:UserPass userpass, 2:string tableName, 3:map<string,set<string>> groups) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);
  void tableOperations_setProperty (1:UserPass userpass, 2:string tableName, 3:string property, 4:string value) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  map<string,string> tableOperations_tableIdMap (1:UserPass userpass);

  //instance operations
  list<PActiveScan> instanceOperations_getActiveScans (1:UserPass userpass, 2:string tserver) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  list<PActiveCompaction> instanceOperations_getActiveCompactions(1:UserPass userpass, 2:string tserver) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  map<string,string> instanceOperations_getSiteConfiguration (1:UserPass userpass) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  map<string,string> instanceOperations_getSystemConfiguration (1:UserPass userpass) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  list<string> instanceOperations_getTabletServers (1:UserPass userpass);
  void instanceOperations_removeProperty (1:UserPass userpass, 2:string property) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  void instanceOperations_setProperty (1:UserPass userpass, 2:string property, 3:string value) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  bool instanceOperations_testClassLoad (1:UserPass userpass, 2:string className, 3:string asTypeName) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);


  //security operations
  bool securityOperations_authenticateUser (1:UserPass userpass, 2:string user, 3:binary password) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  void securityOperations_changeUserAuthorizations (1:UserPass userpass, 2:string user, 3:set<string> authorizations) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  void securityOperations_changeUserPassword (1:UserPass userpass, 2:string user, 3:binary password) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  void securityOperations_createUser (1:UserPass userpass, 2:string user, 3:binary password, 4:set<string> authorizations) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  void securityOperations_dropUser (1:UserPass userpass, 2:string user) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  list<binary> securityOperations_getUserAuthorizations (1:UserPass userpass, 2:string user) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  void securityOperations_grantSystemPermission (1:UserPass userpass, 2:string user, 3:PSystemPermission perm) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  void securityOperations_grantTablePermission (1:UserPass userpass, 2:string user, 3:string table, 4:PTablePermission perm) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  bool securityOperations_hasSystemPermission (1:UserPass userpass, 2:string user, 3:PSystemPermission perm) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  bool securityOperations_hasTablePermission (1:UserPass userpass, 2:string user, 3:string table, 4:PTablePermission perm) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  set<string> securityOperations_listUsers (1:UserPass userpass) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  void securityOperations_revokeSystemPermission (1:UserPass userpass, 2:string user, 3:PSystemPermission perm) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  void securityOperations_revokeTablePermission (1:UserPass userpass, 2:string user, 3:string table, 4:PTablePermission perm) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);


  //scanning

  string createBatchScanner(1:UserPass userpass, 2:string tableName, 3:set<string> authorizations, 4:PIteratorSetting iteratorSetting, 5:list<PRange> range);
  string createScanner(1:UserPass userpass, 2:string tableName, 3:set<string> authorizations, 4:PIteratorSetting iteratorSetting, 5:PRange range);

  bool scanner_hasnext(1:string scanner);
  KeyValueAndPeek scanner_next(1:string scanner);
  PScanResult scanner_next_k(1:string scanner, 2:i32 k);
  void close_scanner(1:string scanner);

  //writing

  void updateAndFlush(1:UserPass userpass, 2:string tableName, 3:map<binary, list<PColumnUpdate>> cells);

  string createWriter(1:UserPass userpass, 2:string tableName);

  oneway void writer_update(1:string writer, 2:map<binary, list<PColumnUpdate>> cells);

  void writer_flush(1:string writer)

  void writer_close(1:string writer)

  void tableOperations_attachIterator (1:UserPass userpass, 2:string tableName, 3:PIteratorSetting setting, 4:set<PIteratorScope> scopes) throws (1:AccumuloSecurityException ouch1, 2:AccumuloException ouch2, 3:TableNotFoundException ouch3);
  void tableOperations_checkIteratorConflicts (1:UserPass userpass, 2:string tableName, 3:PIteratorSetting setting, 4:set<PIteratorScope> scopes) throws (1:AccumuloException ouch1, 2:TableNotFoundException ouch2);
  void tableOperations_clone (1:UserPass userpass, 2:string tableName, 3:string newTableName, 4:bool flush, 5:map<string,string> propertiesToSet, 6:set<string> propertiesToExclude) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3, 4:TableExistsException ouch4);
  void tableOperations_exportTable (1:UserPass userpass, 2:string tableName, 3:string exportDir) throws (1:TableNotFoundException ouch1, 2:AccumuloException ouch2, 3:AccumuloSecurityException ouch3);
  void tableOperations_importTable (1:UserPass userpass, 2:string tableName, 3:string importDir) throws (1:TableExistsException ouch1, 2:AccumuloException ouch2, 3:AccumuloSecurityException ouch3);
  PIteratorSetting tableOperations_getIteratorSetting (1:UserPass userpass, 2:string tableName, 3:string iteratorName, 4:PIteratorScope scope) throws (1:AccumuloSecurityException ouch1, 2:AccumuloException ouch2, 3:TableNotFoundException ouch3);
  map<string,set<PIteratorScope>> tableOperations_listIterators (1:UserPass userpass, 2:string tableName) throws (1:AccumuloSecurityException ouch1, 2:AccumuloException ouch2, 3:TableNotFoundException ouch3);
  void tableOperations_removeIterator (1:UserPass userpass, 2:string tableName, 3:string iterName, 4:set<PIteratorScope> scopes) throws (1:AccumuloSecurityException ouch1, 2:AccumuloException ouch2, 3:TableNotFoundException ouch3);
  set<PRange> tableOperations_splitRangeByTablets (1:UserPass userpass, 2:string tableName, 3:PRange range, 4:i32 maxSplits) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);
  void tableOperations_importDirectory (1:UserPass userpass, 2:string tableName, 3:string importDir, 4:string failureDir, 5:bool setTime) throws (1:TableNotFoundException ouch1, 2:AccumuloException ouch3, 3:AccumuloSecurityException ouch4);

  // utilities
  PRange getRowRange(1:binary row);
  PKey getFollowing(1:PKey key, 2:PPartialKey part);
}
