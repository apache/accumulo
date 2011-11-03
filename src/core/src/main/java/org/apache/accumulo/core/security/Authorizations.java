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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.accumulo.core.util.ByteArraySet;

public class Authorizations implements Iterable<byte[]>, Serializable {
  
  private static final long serialVersionUID = 1L;
  
  private ByteArraySet auths = new ByteArraySet();
  private List<byte[]> authsList = new ArrayList<byte[]>();
  private List<byte[]> immutableList = Collections.unmodifiableList(authsList);
  
  private static final boolean[] validAuthChars = new boolean[256];
  
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
  }
  
  static final boolean isValidAuthChar(byte b) {
    return validAuthChars[0xff & b];
  }
  
  private void checkAuths() {
    
    for (byte[] bs : auths) {
      if (bs.length == 0) {
        throw new IllegalArgumentException("Empty authorization");
      }
      
      for (byte b : bs) {
        if (!isValidAuthChar(b)) {
          throw new IllegalArgumentException("invalid authorization " + new String(bs));
        }
      }
      
      authsList.add(bs);
    }
  }
  
  public Authorizations(Collection<byte[]> authorizations) {
    ArgumentChecker.notNull(authorizations);
    auths = new ByteArraySet(authorizations);
    checkAuths();
  }
  
  public Authorizations(byte[] authorizations) {
    ArgumentChecker.notNull(authorizations);
    if (authorizations.length > 0)
      setAuthorizations(new String(authorizations).split(","));
  }
  
  public Authorizations() {}
  
  public Authorizations(String... authorizations) {
    setAuthorizations(authorizations);
  }
  
  private void setAuthorizations(String... authorizations) {
    ArgumentChecker.notNull(authorizations);
    auths.clear();
    for (String str : authorizations) {
      str = str.trim();
      auths.add(str.getBytes());
    }
    
    checkAuths();
  }
  
  public byte[] getAuthorizationsArray() {
    StringBuilder sb = new StringBuilder();
    String sep = "";
    for (byte[] auth : auths) {
      sb.append(sep);
      sep = ",";
      sb.append(new String(auth));
    }
    
    return sb.toString().getBytes();
  }
  
  public List<byte[]> getAuthorizations() {
    return immutableList;
  }
  
  public String toString() {
    return serialize();
  }
  
  public boolean contains(byte[] auth) {
    return auths.contains(auth);
  }
  
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
  
  public int hashCode() {
    int result = 0;
    for (byte[] b : auths)
      result |= Arrays.hashCode(b);
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
    StringBuilder sb = new StringBuilder();
    String sep = "";
    for (byte[] auth : auths) {
      sb.append(sep);
      sep = ",";
      sb.append(new String(auth));
    }
    
    return sb.toString();
  }
}
