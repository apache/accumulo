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

include "security.thrift"
include "cloudtrace.thrift"

enum TableOperation {
    CREATE,
    DELETE,
    RENAME,
    SET_PROPERTY,
    REMOVE_PROPERTY,
    OFFLINE,
    ONLINE,
    FLUSH,
    PERMISSION
}

enum TableOperationExceptionType {
    EXISTS,
    NOTFOUND,
    OTHER
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
    
    string prepareBulkImport(1:cloudtrace.TInfo tinfo, 2:security.AuthInfo credentials, 3:string dir, 4:string tableName, 5:double errPercent) throws (1:security.ThriftSecurityException sec, 2:ThriftTableOperationException tope);
    void finishBulkImport(1:cloudtrace.TInfo tinfo, 2:security.AuthInfo credentials, 3:string tableName, 4:string lockFile, 5:bool disableGC) throws (1:security.ThriftSecurityException sec, 2:ThriftTableOperationException tope);

    void ping(1:security.AuthInfo credentials) throws (1:security.ThriftSecurityException sec)

    // user management methods
    bool authenticateUser(4:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:string user, 3:binary password) throws (1:security.ThriftSecurityException sec)
    set<string> listUsers(2:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials) throws (1:security.ThriftSecurityException sec)
    void createUser(5:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:string user, 3:binary password, 4:list<binary> authorizations) throws (1:security.ThriftSecurityException sec)
    void dropUser(3:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:string user) throws (1:security.ThriftSecurityException sec)
    void changePassword(4:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:string user, 3:binary password) throws (1:security.ThriftSecurityException sec)
    void changeAuthorizations(4:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:string user, 3:list<binary> authorizations) throws (1:security.ThriftSecurityException sec)
    list<binary> getUserAuthorizations(3:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:string user) throws (1:security.ThriftSecurityException sec)
    bool hasSystemPermission(4:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:string user, 3:byte sysPerm) throws (1:security.ThriftSecurityException sec)
    bool hasTablePermission(5:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:string user, 3:string tableName, 4:byte tblPerm) throws (1:security.ThriftSecurityException sec, 2:ThriftTableOperationException tope)
    void grantSystemPermission(4:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:string user, 3:byte permission) throws (1:security.ThriftSecurityException sec)
    void revokeSystemPermission(4:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:string user, 3:byte permission) throws (1:security.ThriftSecurityException sec)
    void grantTablePermission(5:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:string user, 3:string tableName, 4:byte permission) throws (1:security.ThriftSecurityException sec, 2:ThriftTableOperationException tope)
    void revokeTablePermission(5:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:string user, 3:string tableName, 4:byte permission) throws (1:security.ThriftSecurityException sec, 2:ThriftTableOperationException tope)

}
