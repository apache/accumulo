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
package org.apache.accumulo.core.security;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.impl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

public class CredentialHelper {
  static Logger log = Logger.getLogger(CredentialHelper.class);
  
  public static TCredentials create(String principal, AuthenticationToken token, String instanceID) throws AccumuloSecurityException {
    String className = token.getClass().getName();
    return new TCredentials(principal, className, ByteBuffer.wrap(toBytes(token)), instanceID);
  }
  
  public static String asBase64String(TCredentials cred) throws AccumuloSecurityException {
    return new String(Base64.encodeBase64(asByteArray(cred)), Charset.forName("UTF-8"));
  }
  
  public static byte[] asByteArray(TCredentials cred) throws AccumuloSecurityException {
    TSerializer ts = new TSerializer();
    try {
      return ts.serialize(cred);
    } catch (TException e) {
      // This really shouldn't happen
      log.error(e, e);
      throw new AccumuloSecurityException(cred.getPrincipal(), SecurityErrorCode.SERIALIZATION_ERROR);
    }
  }
  
  public static TCredentials fromBase64String(String string) throws AccumuloSecurityException {
    return fromByteArray(Base64.decodeBase64(string.getBytes(Charset.forName("UTF-8"))));
  }
  
  public static TCredentials fromByteArray(byte[] serializedCredential) throws AccumuloSecurityException {
    if (serializedCredential == null)
      return null;
    TDeserializer td = new TDeserializer();
    try {
      TCredentials toRet = new TCredentials();
      td.deserialize(toRet, serializedCredential);
      return toRet;
    } catch (TException e) {
      // This really shouldn't happen
      log.error(e, e);
      throw new AccumuloSecurityException("unknown", SecurityErrorCode.SERIALIZATION_ERROR);
    }
  }
  
  public static AuthenticationToken extractToken(TCredentials toAuth) throws AccumuloSecurityException {
    return extractToken(toAuth.getTokenClassName(), toAuth.getToken());
  }
  
  public static TCredentials createSquelchError(String principal, AuthenticationToken token, String instanceID) {
    try {
      return create(principal, token, instanceID);
    } catch (AccumuloSecurityException e) {
      log.error(e, e);
      return null;
    }
  }
  
  public static String tokenAsBase64(AuthenticationToken token) throws AccumuloSecurityException {
    return new String(Base64.encodeBase64(toBytes(token)), Charset.forName("UTF-8"));
  }
  
  private static byte[] toBytes(AuthenticationToken token) throws AccumuloSecurityException {
    try {
      ByteArrayOutputStream bais = new ByteArrayOutputStream();
      token.write(new DataOutputStream(bais));
      byte[] serializedToken = bais.toByteArray();
      bais.close();
      return serializedToken;
    } catch (IOException e) {
      log.error(e, e);
      throw new AccumuloSecurityException("unknown", SecurityErrorCode.SERIALIZATION_ERROR);
    }
    
  }
  
  public static AuthenticationToken extractToken(String tokenClass, byte[] token) throws AccumuloSecurityException {
    try {
      Object obj = Class.forName(tokenClass).newInstance();
      if (obj instanceof AuthenticationToken) {
        AuthenticationToken toRet = (AuthenticationToken) obj;
        toRet.readFields(new DataInputStream(new ByteArrayInputStream(token)));
        return toRet;
      }
    } catch (ClassNotFoundException cnfe) {
      log.error(cnfe, cnfe);
    } catch (InstantiationException e) {
      log.error(e, e);
    } catch (IllegalAccessException e) {
      log.error(e, e);
    } catch (IOException e) {
      log.error(e, e);
      throw new AccumuloSecurityException("unknown", SecurityErrorCode.SERIALIZATION_ERROR);
    }
    throw new AccumuloSecurityException("unknown", SecurityErrorCode.INVALID_TOKEN);
  }
  
}
