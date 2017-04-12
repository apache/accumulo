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

struct TCredentials {
    1:string principal,
    2:string tokenClassName,
    3:binary token,
    4:string instanceId
}

struct TAuthenticationTokenIdentifier {
    1:string principal,
    2:optional i32 keyId,
    3:optional i64 issueDate,
    4:optional i64 expirationDate,
    5:optional string instanceId
}

struct TAuthenticationKey {
    1:binary secret,
    2:optional i32 keyId,
    3:optional i64 expirationDate,
    4:optional i64 creationDate
}

struct TDelegationToken {
    1:binary password,
    2:TAuthenticationTokenIdentifier identifier
}

struct TDelegationTokenConfig {
    1:optional i64 lifetime
}