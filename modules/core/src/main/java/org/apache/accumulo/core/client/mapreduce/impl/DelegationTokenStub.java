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
package org.apache.accumulo.core.client.mapreduce.impl;

import static java.util.Objects.requireNonNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

import javax.security.auth.DestroyFailedException;

import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;

/**
 * An internal stub class for passing DelegationToken information out of the Configuration back up to the appropriate implementation for mapreduce or mapred.
 */
public class DelegationTokenStub implements AuthenticationToken {

  private String serviceName;

  public DelegationTokenStub(String serviceName) {
    requireNonNull(serviceName);
    this.serviceName = serviceName;
  }

  public String getServiceName() {
    return serviceName;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void destroy() throws DestroyFailedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isDestroyed() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void init(Properties properties) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<TokenProperty> getProperties() {
    throw new UnsupportedOperationException();
  }

  @Override
  public AuthenticationToken clone() {
    throw new UnsupportedOperationException();
  }
}
