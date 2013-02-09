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
import org.apache.accumulo.core.security.thrift.Credential;
import org.apache.accumulo.core.security.thrift.SecurityErrorCode;
import org.apache.accumulo.core.security.thrift.tokens.SecurityToken;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

public class CredentialHelper {
  static Logger log = Logger.getLogger(CredentialHelper.class);
  
  /**
   * @param principal
   * @param token
   * @param instanceID
   * @return A proper Credential object which can be deserialized by the server
   */
  public static Credential create(String principal, SecurityToken token, String instanceID) throws AccumuloSecurityException {
    String className = token.getClass().getCanonicalName();
    return new Credential(principal, className, ByteBuffer.wrap(toBytes(token)), instanceID);
  }
  
  /**
   * @param cred
   * @return A serialized Credential as a Base64 encoded String
   */
  public static String asBase64String(Credential cred) throws AccumuloSecurityException {
    return new String(Base64.encodeBase64(asByteArray(cred)), Charset.forName("UTF-8"));
  }
  
  /**
   * @param cred
   * @return a serialized Credential
   */
  public static byte[] asByteArray(Credential cred) throws AccumuloSecurityException {
    TSerializer ts = new TSerializer();
    try {
      return ts.serialize(cred);
    } catch (TException e) {
      // This really shouldn't happen
      log.error(e, e);
      throw new AccumuloSecurityException(cred.getPrincipal(), SecurityErrorCode.SERIALIZATION_ERROR);
    }
  }
  
  /**
   * @param string
   * @return
   */
  public static Credential fromBase64String(String string) throws AccumuloSecurityException {
    return fromByteArray(Base64.decodeBase64(string.getBytes(Charset.forName("UTF-8"))));
  }
  
  /**
   * @param decodeBase64
   * @return
   */
  private static Credential fromByteArray(byte[] decodeBase64) throws AccumuloSecurityException {
    TDeserializer td = new TDeserializer();
    try {
      Credential toRet = new Credential();
      td.deserialize(toRet, decodeBase64);
      return toRet;
    } catch (TException e) {
      // This really shouldn't happen
      log.error(e, e);
      throw new AccumuloSecurityException("unknown", SecurityErrorCode.SERIALIZATION_ERROR);
    }
  }
  
  /**
   * @param toAuth
   * @return
   * @throws AccumuloSecurityException
   */
  public static SecurityToken extractToken(Credential toAuth) throws AccumuloSecurityException {
    return extractToken(toAuth.tokenClass, toAuth.getToken());
  }
  
  /**
   * @param systemPrincipal
   * @param systemToken
   * @param instanceID
   * @param b
   * @return
   */
  public static Credential createSquelchError(String principal, SecurityToken token, String instanceID) {
    try {
      return create(principal, token, instanceID);
    } catch (AccumuloSecurityException e) {
      log.error(e, e);
      return null;
    }
  }
  
  /**
   * @param token
   * @return
   * @throws AccumuloSecurityException 
   */
  public static String tokenAsBase64(SecurityToken token) throws AccumuloSecurityException {
    return new String(Base64.encodeBase64(toBytes(token)), Charset.forName("UTF-8"));
  }
  
  /**
   * @param token
   * @return
   * @throws AccumuloSecurityException 
   */
  private static byte[] toBytes(SecurityToken token) throws AccumuloSecurityException {
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

  /**
   * @param tokenClass
   * @param token
   * @return
   * @throws AccumuloSecurityException 
   */
  public static SecurityToken extractToken(String tokenClass, byte[] token) throws AccumuloSecurityException {
    try {
      Object obj = Class.forName(tokenClass).newInstance();
      if (obj instanceof SecurityToken) {
        SecurityToken toRet = (SecurityToken) obj;
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
