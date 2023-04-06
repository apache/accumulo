/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.client.security.tokens;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import javax.security.auth.DestroyFailedException;

/**
 * @since 1.5.0
 */
public class NullToken implements AuthenticationToken {

  @Override
  public void readFields(DataInput arg0) throws IOException {}

  @Override
  public void write(DataOutput arg0) throws IOException {}

  @Override
  public void destroy() throws DestroyFailedException {}

  @Override
  public boolean isDestroyed() {
    return false;
  }

  @Override
  public NullToken clone() {
    try {
      return (NullToken) super.clone();
    } catch (CloneNotSupportedException e) {
      throw new AssertionError("Inconceivable", e);
    }
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof NullToken;
  }

  @Override
  public void init(Properties properties) {}

  @Override
  public Set<TokenProperty> getProperties() {
    return Collections.emptySet();
  }

  @Override
  public int hashCode() {
    return 0;
  }
}
