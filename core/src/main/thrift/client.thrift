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
namespace java org.apache.accumulo.core.client.impl.thrift
namespace cpp org.apache.accumulo.core.client.impl.thrift

include "security.thrift"
include "trace.thrift"

enum TableOperation {
    CREATE,
    DELETE,
    RENAME,
    SET_PROPERTY,
    REMOVE_PROPERTY,
    OFFLINE,
    ONLINE,
    FLUSH,
    PERMISSION,
    CLONE,
    MERGE,
    DELETE_RANGE,
    BULK_IMPORT,
    COMPACT
    IMPORT
    EXPORT
    COMPACT_CANCEL
}

enum TableOperationExceptionType {
    EXISTS,
    NOTFOUND,
    OFFLINE,
    BULK_BAD_INPUT_DIRECTORY,
    BULK_BAD_ERROR_DIRECTORY,
    BAD_RANGE,
    OTHER
}

enum ConfigurationType {
    CURRENT,
    SITE,
    DEFAULT
}

exception ThriftTableOperationException {
    1:string tableId,
    2:string tableName,
    3:TableOperation op,
    4:TableOperationExceptionType type,
    5:string description
}

service ClientService {

    // system management methods
    string getRootTabletLocation()
    string getInstanceId()
    string getZooKeepers()
    
    list<string> bulkImportFiles(1:trace.TInfo tinfo, 8:security.TCredentials credentials, 3:i64 tid, 4:string tableId, 5:list<string> files, 6:string errorDir, 7:bool setTime) throws (1:security.ThriftSecurityException sec, 2:ThriftTableOperationException tope);
    // ensures that nobody is working on the transaction id above
    bool isActive(1:trace.TInfo tinfo, 2:i64 tid),

    void ping(2:security.TCredentials credentials) throws (1:security.ThriftSecurityException sec)

    // user management methods
    set<string> listLocalUsers(2:trace.TInfo tinfo, 3:security.TCredentials credentials) throws (1:security.ThriftSecurityException sec)
    void createLocalUser(5:trace.TInfo tinfo, 6:security.TCredentials credentials, 2:string principal, 3:binary password) throws (1:security.ThriftSecurityException sec)
    void dropLocalUser(3:trace.TInfo tinfo, 4:security.TCredentials credentials, 2:string principal) throws (1:security.ThriftSecurityException sec)
    void changeLocalUserPassword(4:trace.TInfo tinfo, 5:security.TCredentials credentials, 2:string principal, 3:binary password) throws (1:security.ThriftSecurityException sec)

    // authentication-related methods
    bool authenticate(1:trace.TInfo tinfo, 2:security.TCredentials credentials) throws (1:security.ThriftSecurityException sec)
    bool authenticateUser(1:trace.TInfo tinfo, 2:security.TCredentials credentials, 3:security.TCredentials toAuth) throws (1:security.ThriftSecurityException sec)

    // authorization-related methods
    void changeAuthorizations(4:trace.TInfo tinfo, 5:security.TCredentials credentials, 2:string principal, 3:list<binary> authorizations) throws (1:security.ThriftSecurityException sec)
    list<binary> getUserAuthorizations(3:trace.TInfo tinfo, 4:security.TCredentials credentials, 2:string principal) throws (1:security.ThriftSecurityException sec)

    // permissions-related methods
    bool hasSystemPermission(4:trace.TInfo tinfo, 5:security.TCredentials credentials, 2:string principal, 3:byte sysPerm) throws (1:security.ThriftSecurityException sec)
    bool hasTablePermission(5:trace.TInfo tinfo, 6:security.TCredentials credentials, 2:string principal, 3:string tableName, 4:byte tblPerm) throws (1:security.ThriftSecurityException sec, 2:ThriftTableOperationException tope)
    void grantSystemPermission(4:trace.TInfo tinfo, 5:security.TCredentials credentials, 2:string principal, 3:byte permission) throws (1:security.ThriftSecurityException sec)
    void revokeSystemPermission(4:trace.TInfo tinfo, 5:security.TCredentials credentials, 2:string principal, 3:byte permission) throws (1:security.ThriftSecurityException sec)
    void grantTablePermission(5:trace.TInfo tinfo, 6:security.TCredentials credentials, 2:string principal, 3:string tableName, 4:byte permission) throws (1:security.ThriftSecurityException sec, 2:ThriftTableOperationException tope)
    void revokeTablePermission(5:trace.TInfo tinfo, 6:security.TCredentials credentials, 2:string principal, 3:string tableName, 4:byte permission) throws (1:security.ThriftSecurityException sec, 2:ThriftTableOperationException tope)

    // configuration methods
    map<string, string> getConfiguration(2:trace.TInfo tinfo, 3:security.TCredentials credentials, 1:ConfigurationType type);
    map<string, string> getTableConfiguration(1:trace.TInfo tinfo, 3:security.TCredentials credentials, 2:string tableName) throws (1:ThriftTableOperationException tope);
    bool checkClass(1:trace.TInfo tinfo, 4:security.TCredentials credentials, 2:string className, 3:string interfaceMatch);
}

// Only used for a unit test
service ThriftTest {
  bool success();
  bool fails();
  bool throwsError() throws (1:security.ThriftSecurityException ex);
}
