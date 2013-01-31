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
package org.apache.accumulo.core.security.tokens;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.security.thrift.SecurityErrorCode;
import org.apache.accumulo.core.security.thrift.ThriftUserPassToken;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

public class UserPassSerDe implements SecuritySerDe<UserPassToken> {
  private static Logger log = Logger.getLogger(UserPassSerDe.class);
  
  @Override
  public byte[] serialize(UserPassToken token) throws AccumuloSecurityException {
    ThriftUserPassToken t = new ThriftUserPassToken(token.getPrincipal(), ByteBuffer.wrap(token.getPassword()));
    TSerializer serializer = new TSerializer();
    ByteArrayOutputStream bout = null;
    DataOutputStream out = null;
    try {
      bout = new ByteArrayOutputStream();
      out = new DataOutputStream(bout);
      WritableUtils.writeCompressedByteArray(out, serializer.serialize(t));
      return bout.toByteArray();
    } catch (TException te) {
      // This shouldn't happen
      log.error(te);
      throw new AccumuloSecurityException(token.getPrincipal(), SecurityErrorCode.INVALID_TOKEN);
    } catch (IOException e) {
      // This shouldn't happen
      log.error(e);
      throw new AccumuloSecurityException(token.getPrincipal(), SecurityErrorCode.INVALID_TOKEN);
    } finally {
      try {
        if (bout != null)
          bout.close();
        if (out != null)
          out.close();
      } catch (IOException e) {
        log.error(e);
      }
    }
  }
  
  @Override
  public UserPassToken deserialize(byte[] serializedToken) throws AccumuloSecurityException {
    ByteArrayInputStream bin = null;
    DataInputStream in = null;
    try {
      bin = new ByteArrayInputStream(serializedToken);
      in = new DataInputStream(bin);
      
      TDeserializer deserializer = new TDeserializer();
      ThriftUserPassToken obj = new ThriftUserPassToken();
      byte[] tokenBytes;
      tokenBytes = WritableUtils.readCompressedByteArray(in);
      deserializer.deserialize(obj, tokenBytes);
      
      return new UserPassToken(obj.user, obj.getPassword());
    } catch (IOException e) {
      log.error(e);
      throw new AccumuloSecurityException("unknown user", SecurityErrorCode.INVALID_TOKEN);
    } catch (TException e) {
      log.error(e);
      throw new AccumuloSecurityException("unknown user", SecurityErrorCode.INVALID_TOKEN);
    } finally {
      try {
        if (bin != null)
          bin.close();
        if (in != null)
          in.close();
      } catch (IOException e) {
        log.error(e);
      }
    }
  }
}
