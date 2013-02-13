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

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.commons.codec.binary.Base64;

public class Authorizations implements Iterable<byte[]>, Serializable {
  
  private static final long serialVersionUID = 1L;
  
  private Set<ByteSequence> auths = new TreeSet<ByteSequence>();
  private List<byte[]> authsList = new ArrayList<byte[]>();
  private List<byte[]> immutableList = Collections.unmodifiableList(authsList);
  
  private static final boolean[] validAuthChars = new boolean[256];
  
  public static final String HEADER = "!AUTH1:";
  
  static {
    for (int i = 0; i < 256; i++) {
      validAuthChars[i] = false;
    }
    
    for (int i = 'a'; i <= 'z'; i++) {
      validAuthChars[i] = true;
    }
    
    for (int i = 'A'; i <= 'Z'; i++) {
      validAuthChars[i] = true;
    }
    
    for (int i = '0'; i <= '9'; i++) {
      validAuthChars[i] = true;
    }
    
    validAuthChars['_'] = true;
    validAuthChars['-'] = true;
    validAuthChars[':'] = true;
    validAuthChars['.'] = true;
    validAuthChars['/'] = true;
  }
  
  static final boolean isValidAuthChar(byte b) {
    return validAuthChars[0xff & b];
  }
  
  private void checkAuths() {
    
    for (ByteSequence bs : auths) {
      if (bs.length() == 0) {
        throw new IllegalArgumentException("Empty authorization");
      }
      
      authsList.add(bs.toArray());
    }
  }
  
  public Authorizations(Collection<byte[]> authorizations) {
    ArgumentChecker.notNull(authorizations);
    for (byte[] auth : authorizations)
      auths.add(new ArrayByteSequence(auth));
    checkAuths();
  }
  
  public Authorizations(List<ByteBuffer> authorizations) {
    ArgumentChecker.notNull(authorizations);
    for (ByteBuffer buffer : authorizations) {
      auths.add(new ArrayByteSequence(ByteBufferUtil.toBytes(buffer)));
    }
    checkAuths();
  }
  
  /**
   * @param authorizations
   *          a serialized authorizations string produced by {@link #getAuthorizationsArray()} or {@link #serialize()}
   */
  
  public Authorizations(byte[] authorizations) {
    
    ArgumentChecker.notNull(authorizations);
    
    String authsString = new String(authorizations);
    if (authsString.startsWith(HEADER)) {
      // its the new format
      authsString = authsString.substring(HEADER.length());
      if (authsString.length() > 0) {
        for (String encAuth : authsString.split(",")) {
          byte[] auth = Base64.decodeBase64(encAuth.getBytes());
          auths.add(new ArrayByteSequence(auth));
        }
        checkAuths();
      }
    } else {
      // its the old format
      ArgumentChecker.notNull(authorizations);
      if (authorizations.length > 0)
        setAuthorizations(authsString.split(","));
    }
  }
  
  public Authorizations() {}
  
  /**
   * 
   * @param charset
   *          used to convert each authorization to a byte array
   * @param authorizations
   *          array of authorizations
   */
  
  public Authorizations(Charset charset, String... authorizations) {
    setAuthorizations(charset, authorizations);
  }
  
  public Authorizations(String... authorizations) {
    setAuthorizations(authorizations);
  }
  
  private void setAuthorizations(String... authorizations) {
    setAuthorizations(Charset.defaultCharset(), authorizations);
  }
  
  private void setAuthorizations(Charset charset, String... authorizations) {
    ArgumentChecker.notNull(authorizations);
    auths.clear();
    for (String str : authorizations) {
      str = str.trim();
      try {
        auths.add(new ArrayByteSequence(str.getBytes(charset.name())));
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }
    }
    
    checkAuths();
  }
  
  public byte[] getAuthorizationsArray() {
    return serialize().getBytes();
  }
  
  public List<byte[]> getAuthorizations() {
    return immutableList;
  }
  
  public List<ByteBuffer> getAuthorizationsBB() {
    return ByteBufferUtil.toByteBuffers(immutableList);
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    String sep = "";
    for (ByteSequence auth : auths) {
      sb.append(sep);
      sep = ",";
      sb.append(new String(auth.toArray()));
    }
    
    return sb.toString();
  }
  
  public boolean contains(byte[] auth) {
    return auths.contains(new ArrayByteSequence(auth));
  }
  
  public boolean contains(ByteSequence auth) {
    return auths.contains(auth);
  }
  
  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    
    if (o instanceof Authorizations) {
      Authorizations ao = (Authorizations) o;
      
      return auths.equals(ao.auths);
    }
    
    return false;
  }
  
  @Override
  public int hashCode() {
    int result = 0;
    for (ByteSequence b : auths)
      result += b.hashCode();
    return result;
  }
  
  public int size() {
    return auths.size();
  }
  
  public boolean isEmpty() {
    return auths.isEmpty();
  }
  
  @Override
  public Iterator<byte[]> iterator() {
    return immutableList.iterator();
  }
  
  public String serialize() {
    StringBuilder sb = new StringBuilder(HEADER);
    String sep = "";
    for (ByteSequence auth : auths) {
      sb.append(sep);
      sep = ",";
      sb.append(new String(Base64.encodeBase64(auth.toArray())));
    }
    
    return sb.toString();
  }
}
