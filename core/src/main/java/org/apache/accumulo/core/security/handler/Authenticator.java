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
package org.apache.accumulo.core.security.handler;

import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;

public interface Authenticator {
  public AuthenticationToken login(String principal, Properties properties) throws AccumuloSecurityException;
  
  public List<Set<AuthProperty>> getProperties();
  
  public class AuthProperty implements Comparable<AuthProperty>{
    private String key, description;
    private boolean masked;
    
    public AuthProperty(String name, String description, boolean mask) {
      this.key = name;
      this.description = description;
      this.masked = mask;
    }

    public String toString() {
      return this.key + " - " + description;
    }
    
    public String getKey() {
      return this.key;
    }
    
    public String getDescription() {
      return this.description;
    }
    
    public boolean getMask() {
      return this.masked;
    }
    
    public int hashCode() {
      return key.hashCode();
    }
    
    public boolean equals(Object o) {
      if (o instanceof AuthProperty)
        return ((AuthProperty) o).key.equals(key);
      return false;
    }

    @Override
    public int compareTo(AuthProperty o) {
      return key.compareTo(o.key);
    }
  }
}
