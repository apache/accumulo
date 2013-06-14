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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.commons.codec.binary.Base64;

public class Authorizations implements Iterable<byte[]>, Serializable {
  
  private static final long serialVersionUID = 1L;
  
  private Set<ByteSequence> auths = new HashSet<ByteSequence>();
  private List<byte[]> authsList = new ArrayList<byte[]>();
  
  public static final Authorizations EMPTY = new Authorizations();
  
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
    Set<ByteSequence> sortedAuths = new TreeSet<ByteSequence>(auths);
    
    for (ByteSequence bs : sortedAuths) {
      if (bs.length() == 0) {
        throw new IllegalArgumentException("Empty authorization");
      }
      
      authsList.add(bs.toArray());
    }
  }
  
  /**
   * A convenience constructor that accepts a collection of string authorizations that have each already been encoded as UTF-8 bytes.
   * 
   * @see #Authorizations(String...)
   */
  public Authorizations(Collection<byte[]> authorizations) {
    ArgumentChecker.notNull(authorizations);
    for (byte[] auth : authorizations)
      auths.add(new ArrayByteSequence(auth));
    checkAuths();
  }
  
  /**
   * A convenience constructor that accepts a collection of string authorizations that have each already been encoded as UTF-8 bytes.
   * 
   * @see #Authorizations(String...)
   */
  public Authorizations(List<ByteBuffer> authorizations) {
    ArgumentChecker.notNull(authorizations);
    for (ByteBuffer buffer : authorizations) {
      auths.add(new ArrayByteSequence(ByteBufferUtil.toBytes(buffer)));
    }
    checkAuths();
  }
  
  /**
   * Constructs an authorizations object a serialized form. This is NOT a constructor for a set of authorizations of size one.
   * 
   * @param authorizations
   *          a serialized authorizations string produced by {@link #getAuthorizationsArray()} or {@link #serialize()} (converted to UTF-8 bytes)
   */
  public Authorizations(byte[] authorizations) {
    
    ArgumentChecker.notNull(authorizations);
    
    String authsString = new String(authorizations, Constants.UTF8);
    if (authsString.startsWith(HEADER)) {
      // it's the new format
      authsString = authsString.substring(HEADER.length());
      if (authsString.length() > 0) {
        for (String encAuth : authsString.split(",")) {
          byte[] auth = Base64.decodeBase64(encAuth.getBytes(Constants.UTF8));
          auths.add(new ArrayByteSequence(auth));
        }
        checkAuths();
      }
    } else {
      // it's the old format
      ArgumentChecker.notNull(authorizations);
      if (authorizations.length > 0)
        setAuthorizations(authsString.split(","));
    }
  }
  
  /**
   * Constructs an empty set of authorizations.
   * 
   * @see #Authorizations(String...)
   */
  public Authorizations() {}
  
  /**
   * Constructs an authorizations object from a set of human-readable authorizations.
   * 
   * @param authorizations
   *          array of authorizations
   */
  public Authorizations(String... authorizations) {
    setAuthorizations(authorizations);
  }
  
  private void setAuthorizations(String... authorizations) {
    ArgumentChecker.notNull(authorizations);
    auths.clear();
    for (String str : authorizations) {
      str = str.trim();
      auths.add(new ArrayByteSequence(str.getBytes(Constants.UTF8)));
    }
    
    checkAuths();
  }
  
  /**
   * Retrieve a serialized form of the underlying set of authorizations.
   * 
   * @see #Authorizations(byte[])
   */
  public byte[] getAuthorizationsArray() {
    return serialize().getBytes(Constants.UTF8);
  }
  
  /**
   * Retrieve authorizations as a list of strings that have been encoded as UTF-8 bytes.
   * 
   * @see #Authorizations(Collection)
   */
  public List<byte[]> getAuthorizations() {
    ArrayList<byte[]> copy = new ArrayList<byte[]>(authsList.size());
    for (byte[] auth : authsList) {
      byte[] bytes = new byte[auth.length];
      System.arraycopy(auth, 0, bytes, 0, auth.length);
      copy.add(bytes);
    }
    return Collections.unmodifiableList(copy);
  }
  
  /**
   * Retrieve authorizations as a list of strings that have been encoded as UTF-8 bytes.
   * 
   * @see #Authorizations(List)
   */
  public List<ByteBuffer> getAuthorizationsBB() {
    return ByteBufferUtil.toImmutableByteBufferList(getAuthorizations());
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    String sep = "";
    for (ByteSequence auth : auths) {
      sb.append(sep);
      sep = ",";
      sb.append(new String(auth.toArray(), Constants.UTF8));
    }
    
    return sb.toString();
  }
  
  /**
   * Checks for the existence of this UTF-8 encoded authorization.
   */
  public boolean contains(byte[] auth) {
    return auths.contains(new ArrayByteSequence(auth));
  }
  
  /**
   * Checks for the existence of this UTF-8 encoded authorization.
   */
  public boolean contains(ByteSequence auth) {
    return auths.contains(auth);
  }
  
  /**
   * Checks for the existence of this authorization.
   */
  public boolean contains(String auth) {
    return auths.contains(auth.getBytes(Constants.UTF8));
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
    return getAuthorizations().iterator();
  }
  
  /**
   * Returns a serialized form of these authorizations. Convert to UTF-8 bytes to deserialize with {@link #Authorizations(byte[])}
   */
  public String serialize() {
    StringBuilder sb = new StringBuilder(HEADER);
    String sep = "";
    for (byte[] auth : authsList) {
      sb.append(sep);
      sep = ",";
      sb.append(new String(Base64.encodeBase64(auth), Constants.UTF8));
    }
    
    return sb.toString();
  }
}
