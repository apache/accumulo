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
package org.apache.accumulo.proxy;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.security.thrift.SecurityErrorCode;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

public class CredentialHelper {

    public static byte[] asByteArray(AuthInfo cred) throws AccumuloSecurityException {
        TSerializer ts = new TSerializer();
        try {
            return ts.serialize(cred);
        } catch (TException e) {
            // This really shouldn't happen
            throw new AccumuloSecurityException(cred.getUser(), SecurityErrorCode.DEFAULT_SECURITY_ERROR, e);
        }
    }

    public static AuthInfo fromByteArray(byte[] serializedCredential) throws AccumuloSecurityException {
        if (serializedCredential == null)
            return null;
        TDeserializer td = new TDeserializer();
        try {
            AuthInfo toRet = new AuthInfo();
            td.deserialize(toRet, serializedCredential);
            return toRet;
        } catch (TException e) {
            // This really shouldn't happen
            throw new AccumuloSecurityException("unknown", SecurityErrorCode.DEFAULT_SECURITY_ERROR, e);
        }
    }

}
