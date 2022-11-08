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
package org.apache.accumulo.core.clientImpl;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.admin.CloneConfiguration;

import com.google.common.base.Preconditions;

/**
 * A {@link CloneConfiguration} implementation which also implements the builder thereof
 *
 * @since 1.10 and 2.1
 */
public class CloneConfigurationImpl implements CloneConfiguration, CloneConfiguration.Builder {

  // This boolean allows building an immutable CloneConfiguration object without creating
  // separate Builder and CloneConfiguration objects. This is done to reduce object creation and
  // copying. This could easily be changed to two objects without changing the interfaces.
  private boolean built = false;

  // determines if memory is flushed in the source table before cloning.
  private boolean flush = true;

  // the sources table properties are copied, this allows overriding of those properties
  private Map<String,String> propertiesToSet = null;

  // do not copy these properties from the source table, just revert to system defaults
  private Set<String> propertiesToExclude = null;

  // do not bring the table online after cloning
  private boolean keepOffline = false;

  public CloneConfigurationImpl() {}

  @Override
  public boolean isFlush() {
    Preconditions.checkState(built);
    return flush;
  }

  @Override
  public Map<String,String> getPropertiesToSet() {
    Preconditions.checkState(built);
    return (propertiesToSet == null ? Collections.emptyMap()
        : Collections.unmodifiableMap(propertiesToSet));
  }

  @Override
  public Set<String> getPropertiesToExclude() {
    Preconditions.checkState(built);
    return (propertiesToExclude == null ? Collections.emptySet()
        : Collections.unmodifiableSet(propertiesToExclude));
  }

  @Override
  public boolean isKeepOffline() {
    Preconditions.checkState(built);
    return keepOffline;
  }

  @Override
  public Builder setFlush(boolean flush) {
    Preconditions.checkState(!built);
    this.flush = flush;
    return this;
  }

  @Override
  public Builder setPropertiesToSet(Map<String,String> propertiesToSet) {
    Preconditions.checkState(!built);
    this.propertiesToSet = propertiesToSet;
    return this;
  }

  @Override
  public Builder setPropertiesToExclude(Set<String> propertiesToExclude) {
    Preconditions.checkState(!built);
    this.propertiesToExclude = propertiesToExclude;
    return this;
  }

  @Override
  public Builder setKeepOffline(boolean keepOffline) {
    Preconditions.checkState(!built);
    this.keepOffline = keepOffline;
    return this;
  }

  @Override
  public CloneConfiguration build() {
    Preconditions.checkState(!built);
    built = true;
    return this;
  }

  @Override
  public String toString() {
    return "{flush=" + flush + ", propertiesToSet=" + propertiesToSet + ", propertiesToExclude="
        + propertiesToExclude + ", keepOffline=" + keepOffline + ", built=" + built + "}";
  }
}
