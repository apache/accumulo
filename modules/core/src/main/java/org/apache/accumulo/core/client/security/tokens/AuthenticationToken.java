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
package org.apache.accumulo.core.client.security.tokens;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.security.auth.DestroyFailedException;
import javax.security.auth.Destroyable;

import org.apache.hadoop.io.Writable;

/**
 *
 * @since 1.5.0
 */
public interface AuthenticationToken extends Writable, Destroyable, Cloneable {

  /**
   * A utility class to serialize/deserialize {@link AuthenticationToken} objects.<br>
   * Unfortunately, these methods are provided in an inner-class, to avoid breaking the interface API.
   *
   * @since 1.6.0
   */
  public static final class AuthenticationTokenSerializer {
    /**
     * A convenience method to create tokens from serialized bytes, created by {@link #serialize(AuthenticationToken)}
     * <p>
     * The specified tokenType will be instantiated, and used to deserialize the decoded bytes. The resulting object will then be returned to the caller.
     *
     * @param tokenType
     *          the token class to use to deserialize the bytes
     * @param tokenBytes
     *          the token-specific serialized bytes
     * @return an {@link AuthenticationToken} instance of the type specified by tokenType
     * @see #serialize(AuthenticationToken)
     */
    public static <T extends AuthenticationToken> T deserialize(Class<T> tokenType, byte[] tokenBytes) {
      T type = null;
      try {
        type = tokenType.newInstance();
      } catch (Exception e) {
        throw new IllegalArgumentException("Cannot instantiate " + tokenType.getName(), e);
      }
      ByteArrayInputStream bais = new ByteArrayInputStream(tokenBytes);
      DataInputStream in = new DataInputStream(bais);
      try {
        type.readFields(in);
      } catch (IOException e) {
        throw new IllegalArgumentException("Cannot deserialize provided byte array as class " + tokenType.getName(), e);
      }
      try {
        in.close();
      } catch (IOException e) {
        throw new IllegalStateException("Shouldn't happen", e);
      }
      return type;
    }

    /**
     * An alternate version of {@link #deserialize(Class, byte[])} that accepts a token class name rather than a token class.
     *
     * @param tokenClassName
     *          the fully-qualified class name to be returned
     * @see #serialize(AuthenticationToken)
     */
    public static AuthenticationToken deserialize(String tokenClassName, byte[] tokenBytes) {
      Class<? extends AuthenticationToken> tokenType = null;
      try {
        @SuppressWarnings("unchecked")
        Class<? extends AuthenticationToken> tmpTokenType = (Class<? extends AuthenticationToken>) Class.forName(tokenClassName);
        tokenType = tmpTokenType;
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Class not available " + tokenClassName, e);
      }
      return deserialize(tokenType, tokenBytes);
    }

    /**
     * A convenience method to serialize tokens.
     * <p>
     * The provided {@link AuthenticationToken} will be serialized to bytes by its own implementation and returned to the caller.
     *
     * @param token
     *          the token to serialize
     * @return a serialized representation of the provided {@link AuthenticationToken}
     * @see #deserialize(Class, byte[])
     */
    public static byte[] serialize(AuthenticationToken token) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(baos);
      try {
        token.write(out);
      } catch (IOException e) {
        throw new RuntimeException("Bug found in serialization code", e);
      }
      byte[] bytes = baos.toByteArray();
      try {
        out.close();
      } catch (IOException e) {
        throw new IllegalStateException("Shouldn't happen with ByteArrayOutputStream", e);
      }
      return bytes;
    }
  }

  class Properties implements Destroyable, Map<String,char[]> {

    private boolean destroyed = false;
    private HashMap<String,char[]> map = new HashMap<>();

    private void checkDestroyed() {
      if (destroyed)
        throw new IllegalStateException();
    }

    public char[] put(String key, CharSequence value) {
      checkDestroyed();
      char[] toPut = new char[value.length()];
      for (int i = 0; i < value.length(); i++)
        toPut[i] = value.charAt(i);
      return map.put(key, toPut);
    }

    public void putAllStrings(Map<String,? extends CharSequence> map) {
      checkDestroyed();
      for (Map.Entry<String,? extends CharSequence> entry : map.entrySet()) {
        put(entry.getKey(), entry.getValue());
      }
    }

    @Override
    public void destroy() throws DestroyFailedException {
      for (String key : this.keySet()) {
        char[] val = this.get(key);
        Arrays.fill(val, (char) 0);
      }
      this.clear();
      destroyed = true;
    }

    @Override
    public boolean isDestroyed() {
      return destroyed;
    }

    @Override
    public int size() {
      checkDestroyed();
      return map.size();
    }

    @Override
    public boolean isEmpty() {
      checkDestroyed();
      return map.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
      checkDestroyed();
      return map.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
      checkDestroyed();
      return map.containsValue(value);
    }

    @Override
    public char[] get(Object key) {
      checkDestroyed();
      return map.get(key);
    }

    @Override
    public char[] put(String key, char[] value) {
      checkDestroyed();
      return map.put(key, value);
    }

    @Override
    public char[] remove(Object key) {
      checkDestroyed();
      return map.remove(key);
    }

    @Override
    public void putAll(Map<? extends String,? extends char[]> m) {
      checkDestroyed();
      map.putAll(m);
    }

    @Override
    public void clear() {
      checkDestroyed();
      map.clear();
    }

    @Override
    public Set<String> keySet() {
      checkDestroyed();
      return map.keySet();
    }

    @Override
    public Collection<char[]> values() {
      checkDestroyed();
      return map.values();
    }

    @Override
    public Set<Map.Entry<String,char[]>> entrySet() {
      checkDestroyed();
      return map.entrySet();
    }
  }

  static class TokenProperty implements Comparable<TokenProperty> {
    private String key, description;
    private boolean masked;

    public TokenProperty(String name, String description, boolean mask) {
      this.key = name;
      this.description = description;
      this.masked = mask;
    }

    @Override
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

    @Override
    public int hashCode() {
      return key.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof TokenProperty)
        return ((TokenProperty) o).key.equals(key);
      return false;
    }

    @Override
    public int compareTo(TokenProperty o) {
      return key.compareTo(o.key);
    }
  }

  public void init(Properties properties);

  public Set<TokenProperty> getProperties();

  public AuthenticationToken clone();
}
