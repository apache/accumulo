/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
namespace java org.apache.accumulo.core.clientImpl.thrift
namespace cpp org.apache.accumulo.core.clientImpl.thrift

include "security.thrift"

enum TableOperation {
  CREATE
  DELETE
  RENAME
  SET_PROPERTY
  REMOVE_PROPERTY
  OFFLINE
  ONLINE
  FLUSH
  PERMISSION
  CLONE
  MERGE
  DELETE_RANGE
  BULK_IMPORT
  COMPACT
  IMPORT
  EXPORT
  COMPACT_CANCEL
  SET_HOSTING_GOAL
  SPLIT
}

enum TableOperationExceptionType {
  EXISTS
  NOTFOUND
  OFFLINE
  BULK_BAD_INPUT_DIRECTORY
  OBSOLETE_BULK_BAD_ERROR_DIRECTORY
  BAD_RANGE
  OTHER
  NAMESPACE_EXISTS
  NAMESPACE_NOTFOUND
  INVALID_NAME
  OBSOLETE_BULK_BAD_LOAD_MAPPING
  BULK_CONCURRENT_MERGE
}

enum ConfigurationType {
  CURRENT
  SITE
  DEFAULT
}

enum SecurityErrorCode {
  DEFAULT_SECURITY_ERROR = 0
  BAD_CREDENTIALS = 1
  PERMISSION_DENIED = 2
  USER_DOESNT_EXIST = 3
  CONNECTION_ERROR = 4
  USER_EXISTS = 5
  GRANT_INVALID = 6
  BAD_AUTHORIZATIONS = 7
  INVALID_INSTANCEID = 8
  TABLE_DOESNT_EXIST = 9
  UNSUPPORTED_OPERATION = 10
  INVALID_TOKEN = 11
  AUTHENTICATOR_FAILED = 12
  AUTHORIZOR_FAILED = 13
  PERMISSIONHANDLER_FAILED = 14
  TOKEN_EXPIRED = 15
  SERIALIZATION_ERROR = 16
  INSUFFICIENT_PROPERTIES = 17
  NAMESPACE_DOESNT_EXIST = 18
}

exception ThriftSecurityException {
  1:string user
  2:SecurityErrorCode code
}

exception ThriftTableOperationException {
  1:string tableId
  2:string tableName
  3:TableOperation op
  4:TableOperationExceptionType type
  5:string description
}

exception ThriftNotActiveServiceException {
  1:string serv
  2:string description
}

exception ThriftConcurrentModificationException {
  1:string description
}

struct TDiskUsage {
  1:list<string> tables
  2:i64 usage
}

struct TVersionedProperties {
   1:i64 version
   2:map<string, string> properties
}

struct TInfo {
  1:map<string,string> headers
}

enum THostingGoal {
  ALWAYS
  NEVER
  ONDEMAND
}

service ClientService {

  // system management methods
  string getRootTabletLocation()
  string getInstanceId()
  string getZooKeepers()

  void ping(
    2:security.TCredentials credentials
  ) throws (
    1:ThriftSecurityException sec
  )

  list<TDiskUsage> getDiskUsage(
    2:set<string> tables
    1:security.TCredentials credentials
  ) throws (
    1:ThriftSecurityException sec
    2:ThriftTableOperationException toe
  )

  // user management methods
  set<string> listLocalUsers(
    2:TInfo tinfo
    3:security.TCredentials credentials
  ) throws (
    1:ThriftSecurityException sec
  )

  void createLocalUser(
    5:TInfo tinfo
    6:security.TCredentials credentials
    2:string principal
    3:binary password
  ) throws (
    1:ThriftSecurityException sec
  )

  void dropLocalUser(
    3:TInfo tinfo
    4:security.TCredentials credentials
    2:string principal
  ) throws (
    1:ThriftSecurityException sec
  )

  void changeLocalUserPassword(
    4:TInfo tinfo
    5:security.TCredentials credentials
    2:string principal
    3:binary password
  ) throws (
    1:ThriftSecurityException sec
  )

  // authentication-related methods
  bool authenticate(
    1:TInfo tinfo
    2:security.TCredentials credentials
  ) throws (
    1:ThriftSecurityException sec
  )

  bool authenticateUser(
    1:TInfo tinfo
    2:security.TCredentials credentials
    3:security.TCredentials toAuth
  ) throws (
    1:ThriftSecurityException sec
  )

  // authorization-related methods
  void changeAuthorizations(
    4:TInfo tinfo
    5:security.TCredentials credentials
    2:string principal
    3:list<binary> authorizations
  ) throws (
    1:ThriftSecurityException sec
  )

  list<binary> getUserAuthorizations(
    3:TInfo tinfo
    4:security.TCredentials credentials
    2:string principal
  ) throws (
    1:ThriftSecurityException sec
  )

  // permissions-related methods
  bool hasSystemPermission(
    4:TInfo tinfo
    5:security.TCredentials credentials
    2:string principal
    3:i8 sysPerm
  ) throws (
    1:ThriftSecurityException sec
  )

  bool hasTablePermission(
    5:TInfo tinfo
    6:security.TCredentials credentials
    2:string principal
    3:string tableName
    4:i8 tblPerm
  ) throws (
    1:ThriftSecurityException sec
    2:ThriftTableOperationException tope
  )

  bool hasNamespacePermission(
    1:TInfo tinfo
    2:security.TCredentials credentials
    3:string principal
    4:string ns
    5:i8 tblNspcPerm
  ) throws (
    1:ThriftSecurityException sec
    2:ThriftTableOperationException tope
  )

  void grantSystemPermission(
    4:TInfo tinfo
    5:security.TCredentials credentials
    2:string principal
    3:i8 permission
  ) throws (
    1:ThriftSecurityException sec
  )

  void revokeSystemPermission(
    4:TInfo tinfo
    5:security.TCredentials credentials
    2:string principal
    3:i8 permission
  ) throws (
    1:ThriftSecurityException sec
  )

  void grantTablePermission(
    5:TInfo tinfo
    6:security.TCredentials credentials
    2:string principal
    3:string tableName
    4:i8 permission
  ) throws (
    1:ThriftSecurityException sec
    2:ThriftTableOperationException tope
  )

  void revokeTablePermission(
    5:TInfo tinfo
    6:security.TCredentials credentials
    2:string principal
    3:string tableName
    4:i8 permission
  ) throws (
    1:ThriftSecurityException sec
    2:ThriftTableOperationException tope
  )

  void grantNamespacePermission(
    1:TInfo tinfo
    2:security.TCredentials credentials
    3:string principal
    4:string ns
    5:i8 permission
  ) throws (
    1:ThriftSecurityException sec
    2:ThriftTableOperationException tope
  )

  void revokeNamespacePermission(
    1:TInfo tinfo
    2:security.TCredentials credentials
    3:string principal
    4:string ns
    5:i8 permission
  ) throws (
    1:ThriftSecurityException sec
    2:ThriftTableOperationException tope
  )

  // configuration methods
  map<string, string> getConfiguration(
    2:TInfo tinfo
    3:security.TCredentials credentials
    1:ConfigurationType type
  ) throws (
    1:ThriftSecurityException sec
  )

  map<string, string> getSystemProperties(
    1:TInfo tinfo
    2:security.TCredentials credentials
  ) throws (
    1:ThriftSecurityException sec
  )

  TVersionedProperties getVersionedSystemProperties(
    1:TInfo tinfo
    2:security.TCredentials credentials
  ) throws (
    1:ThriftSecurityException sec
  )

  map<string, string> getTableConfiguration(
    1:TInfo tinfo
    3:security.TCredentials credentials
    2:string tableName
  ) throws (
    1:ThriftTableOperationException tope
    2:ThriftSecurityException sec
  )

  map<string, string> getTableProperties(
    1:TInfo tinfo
    3:security.TCredentials credentials
    2:string tableName
  ) throws (
    1:ThriftTableOperationException tope
    2:ThriftSecurityException sec
  )

  TVersionedProperties getVersionedTableProperties(
    1:TInfo tinfo
    3:security.TCredentials credentials
    2:string tableName
  ) throws (
    1:ThriftTableOperationException tope
    2:ThriftSecurityException sec
  )

  map<string, string> getNamespaceConfiguration(
    1:TInfo tinfo
    2:security.TCredentials credentials
    3:string ns
  ) throws (
    1:ThriftTableOperationException tope
    2:ThriftSecurityException sec
  )

  map<string, string> getNamespaceProperties(
    1:TInfo tinfo
    2:security.TCredentials credentials
    3:string ns
  ) throws (
    1:ThriftTableOperationException tope
    2:ThriftSecurityException sec
  )

  TVersionedProperties getVersionedNamespaceProperties(
    1:TInfo tinfo
    2:security.TCredentials credentials
    3:string ns
  ) throws (
    1:ThriftTableOperationException tope
    2:ThriftSecurityException sec
  )

  bool checkClass(
    1:TInfo tinfo
    4:security.TCredentials credentials
    2:string className
    3:string interfaceMatch
  )

  bool checkTableClass(
    1:TInfo tinfo
    5:security.TCredentials credentials
    2:string tableId
    3:string className
    4:string interfaceMatch
  ) throws (
    1:ThriftSecurityException sec
    2:ThriftTableOperationException tope
  )

  bool checkNamespaceClass(
    1:TInfo tinfo
    2:security.TCredentials credentials
    3:string namespaceId
    4:string className
    5:string interfaceMatch
  ) throws (
    1:ThriftSecurityException sec
    2:ThriftTableOperationException tope
  )

}

// Only used for a unit test
service ThriftTest {

  bool success()
  bool fails()
  bool throwsError() throws (
    1:ThriftSecurityException ex
  )

}
