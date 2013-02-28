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
namespace java org.apache.accumulo.core.security.thrift
namespace cpp org.apache.accumulo.core.security.thrift

/**
@deprecated since 1.5, see org.apache.accumulo.core.client.impl.thrift.SecurityErrorCode
*/
enum SecurityErrorCode {
    DEFAULT_SECURITY_ERROR = 0,
    BAD_CREDENTIALS = 1,
    PERMISSION_DENIED = 2,
    USER_DOESNT_EXIST = 3,
    CONNECTION_ERROR = 4,
    USER_EXISTS = 5,
    GRANT_INVALID = 6,
    BAD_AUTHORIZATIONS = 7,
    INVALID_INSTANCEID = 8,
    TABLE_DOESNT_EXIST = 9,
    UNSUPPORTED_OPERATION = 10,
    INVALID_TOKEN = 11,
    AUTHENTICATOR_FAILED = 12,
    AUTHORIZOR_FAILED = 13,
    PERMISSIONHANDLER_FAILED = 14,
    TOKEN_EXPIRED = 15
    SERIALIZATION_ERROR = 16;
    INSUFFICIENT_PROPERTIES = 17;
}

/**
@deprecated since 1.5
*/
struct AuthInfo {
    1:string user,
    2:binary password,
    3:string instanceId
}

struct TCredentials {
    1:string principal,
    2:string tokenClassName,
    3:binary token,
    4:string instanceId
}

/**
@deprecated since 1.5, see org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException
*/
exception ThriftSecurityException {
    1:string user,
    2:SecurityErrorCode code
}
