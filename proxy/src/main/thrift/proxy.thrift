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

struct PColumn {
	1:binary colFamily;
	2:binary colQualifier;
	3:optional binary colVisibility;
	4:optional i64 timestamp;
}

struct PColumnUpdate {
	1:binary colFamily;
	2:binary colQualifier;
	3:optional binary colVisibility;
	4:optional i64 timestamp;
	5:binary value;
}

struct PKeyValue {
	1:PKey key,
	2:binary value
}

struct PScanResult {
	1:list<PKeyValue> results,
	2:bool more
}

// Ranges by default assume that start is inclusive and that stop is exclusive.
// To make an inclusive stop value, (resp. exclusive start value), use the successor of the key
// desired. To obtain a successor key, append a binary 0 to the most-specific value that is defined in the key.
// (See the method org.apache.accumulo.core.data.Key.followingKey(PartialKey part) )

struct PRange {
        1:PKey start,
        2:PKey stop,
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
  void tableOperations_addSplits (1:UserPass userpass, 2:string tableName, 3:set<string> splits) throws (1:TableNotFoundException ouch1, 2:AccumuloException ouch2, 3:AccumuloSecurityException ouch3);
  void tableOperations_clearLocatorCache (1:UserPass userpass, 2:string tableName) throws (1:TableNotFoundException ouch1);
  void tableOperations_compact (1:UserPass userpass, 2:string tableName, 3:string startRow, 4:string endRow, 5:bool flush, 6:bool wait) throws (1:AccumuloSecurityException ouch1, 2:TableNotFoundException ouch2, 3:AccumuloException ouch3);
  void tableOperations_create (1:UserPass userpass, 2:string tableName) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableExistsException ouch3);
  void tableOperations_delete (1:UserPass userpass, 2:string tableName) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);
  void tableOperations_deleteRows (1:UserPass userpass, 2:string tableName, 3:string startRow, 4:string endRow) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);
  bool tableOperations_exists (1:UserPass userpass, 2:string tableName);
  void tableOperations_flush (1:UserPass userpass, 2:string tableName) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  map<string,set<string>> tableOperations_getLocalityGroups (1:UserPass userpass, 2:string tableName) throws (1:AccumuloException ouch1, 2:TableNotFoundException ouch2);
  string tableOperations_getMaxRow (1:UserPass userpass, 2:string tableName, 3:list<binary> auths, 4:string startRow, 5:bool startInclusive, 6:string endRow, 7:bool endInclusive) throws (1:TableNotFoundException ouch1, 2:AccumuloException ouch2, 3:AccumuloSecurityException ouch3);
  map<string,string> tableOperations_getProperties (1:UserPass userpass, 2:string tableName) throws (1:AccumuloException ouch1, 2:TableNotFoundException ouch2);
  list<string> tableOperations_getSplits (1:UserPass userpass, 2:string tableName, 3:i32 maxSplits) throws (1:TableNotFoundException ouch1);
  set<string> tableOperations_list (1:UserPass userpass);
  map<string,i32> tableOperations_listConstraints (1:UserPass userpass, 2:string tableName) throws (1:AccumuloException ouch1, 2:TableNotFoundException ouch2);
  void tableOperations_merge (1:UserPass userpass, 2:string tableName, 3:string startRow, 4:string endRow) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);
  void tableOperations_offline (1:UserPass userpass, 2:string tableName) throws (1:AccumuloSecurityException ouch1, 2:AccumuloException ouch2, 3:TableNotFoundException ouch3);
  void tableOperations_online (1:UserPass userpass, 2:string tableName) throws (1:AccumuloSecurityException ouch1, 2:AccumuloException ouch2, 3:TableNotFoundException ouch3);
  void tableOperations_removeConstraint (1:UserPass userpass, 2:string tableName, 3:i32 constraint) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  void tableOperations_removeProperty (1:UserPass userpass, 2:string tableName, 3:string property) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  void tableOperations_rename (1:UserPass userpass, 2:string oldTableName, 3:string newTableName) throws (1:AccumuloSecurityException ouch1, 2:TableNotFoundException ouch2, 3:AccumuloException ouch3, 4:TableExistsException ouch4);
  void tableOperations_setLocalityGroups (1:UserPass userpass, 2:string tableName, 3:map<string,set<string>> groups) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);
  void tableOperations_setProperty (1:UserPass userpass, 2:string tableName, 3:string property, 4:string value) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
  map<string,string> tableOperations_tableIdMap (1:UserPass userpass);

  //instance operations
  //list<E> instanceOperations_getActiveScans (1:UserPass userpass, 2:string tserver) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2);
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

  //this method is guaranteed to perform an atomic update on all cells with the same row.
  void updateAndFlush(1:UserPass userpass, 2:string tableName, 3:map<binary, list<PColumnUpdate>> cells, 4:map<binary, list<PColumn>> deletedCells);


  //this method creates a persistent writer. use writer_update to perform updates with the returned cookie.
  string createWriter(1:UserPass userpass, 2:string tableName);

  //this method is guaranteed to perform an atomic update on all cells with the same row.
  oneway void writer_update(1:string writer, 2:map<binary, list<PColumnUpdate>> cells, 3:map<binary, list<PColumn>> deletedCells);

  void writer_flush(1:string writer)

  void writer_close(1:string writer)

  //  void tableOperations_attachIterator (1:UserPass userpass, 2:string arg2, 3:PIteratorSetting arg3) throws (1:AccumuloSecurityException ouch1, 2:AccumuloException ouch2, 3:TableNotFoundException ouch3);
  //  void tableOperations_attachIterator (1:UserPass userpass, 2:string arg2, 3:PIteratorSetting arg3, 4:EnumSet<E> arg4) throws (1:AccumuloSecurityException ouch1, 2:AccumuloException ouch2, 3:TableNotFoundException ouch3);
  //  void tableOperations_checkIteratorConflicts (1:UserPass userpass, 2:string arg2, 3:PIteratorSetting arg3, 4:EnumSet<E> arg4) throws (1:AccumuloException ouch1, 2:TableNotFoundException ouch2);
  //  void tableOperations_clone (1:UserPass userpass, 2:string arg2, 3:string arg3, 4:bool arg4, 5:map<K,V> arg5, 6:set<E> arg6) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3, 4:TableExistsException ouch4);
  //  void tableOperations_compact (1:UserPass userpass, 2:string arg2, 3:string arg3, 4:string arg4, 5:list<E> arg5, 6:bool arg6, 7:bool arg7) throws (1:AccumuloSecurityException ouch1, 2:TableNotFoundException ouch2, 3:AccumuloException ouch3);
  //  void tableOperations_create (1:UserPass userpass, 2:string arg2, 3:bool arg3) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableExistsException ouch3);
  //  void tableOperations_create (1:UserPass userpass, 2:string arg2, 3:bool arg3, 4:TimeType arg4) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableExistsException ouch3);
  //  void tableOperations_exportTable (1:UserPass userpass, 2:string arg2, 3:string arg3) throws (1:TableNotFoundException ouch1, 2:AccumuloException ouch2, 3:AccumuloSecurityException ouch3);
  //  void tableOperations_flush (1:UserPass userpass, 2:string arg2, 3:string arg3, 4:string arg4, 5:bool arg5) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);
  //  PIteratorSetting tableOperations_getIteratorSetting (1:UserPass userpass, 2:string arg2, 3:string arg3, 4:IteratorScope arg4) throws (1:AccumuloSecurityException ouch1, 2:AccumuloException ouch2, 3:TableNotFoundException ouch3);
  //  list<E> tableOperations_getSplits (1:UserPass userpass, 2:string arg2) throws (1:TableNotFoundException ouch1);
  //  map<K,V> tableOperations_listIterators (1:UserPass userpass, 2:string arg2) throws (1:AccumuloSecurityException ouch1, 2:AccumuloException ouch2, 3:TableNotFoundException ouch3);
  //  void tableOperations_removeIterator (1:UserPass userpass, 2:string arg2, 3:string arg3, 4:EnumSet<E> arg4) throws (1:AccumuloSecurityException ouch1, 2:AccumuloException ouch2, 3:TableNotFoundException ouch3);
  //  set<E> tableOperations_splitRangeByTablets (1:UserPass userpass, 2:string arg2, 3:Range arg3, 4:int arg4) throws (1:AccumuloException ouch1, 2:AccumuloSecurityException ouch2, 3:TableNotFoundException ouch3);
  //  void tableOperations_importDirectory (1:UserPass userpass, 2:string arg2, 3:string arg3, 4:string arg4, 5:bool arg5) throws (1:TableNotFoundException ouch1, 2:IOException ouch2, 3:AccumuloException ouch3, 4:AccumuloSecurityException ouch4);
  //  void tableOperations_importTable (1:UserPass userpass, 2:string arg2, 3:string arg3) throws (1:TableExistsException ouch1, 2:AccumuloException ouch2, 3:AccumuloSecurityException ouch3);

}
