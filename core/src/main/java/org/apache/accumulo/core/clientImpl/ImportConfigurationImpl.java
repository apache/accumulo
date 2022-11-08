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

import org.apache.accumulo.core.client.admin.ImportConfiguration;

import com.google.common.base.Preconditions;

public class ImportConfigurationImpl implements ImportConfiguration, ImportConfiguration.Builder {
  private boolean built = false;
  private boolean keepOffline = false;
  private boolean keepMappingsFile = false;

  private final static String BUILT_ERROR_MSG = "ImportConfiguration was already built";
  private final static String NOT_BUILT_ERROR_MSG = "ImportConfiguration was not built yet";

  @Override
  public ImportConfiguration.Builder setKeepOffline(boolean keepOffline) {
    Preconditions.checkState(!built, BUILT_ERROR_MSG);
    this.keepOffline = keepOffline;
    return this;
  }

  @Override
  public Builder setKeepMappings(boolean keepMappings) {
    Preconditions.checkState(!built, BUILT_ERROR_MSG);
    this.keepMappingsFile = keepMappings;
    return this;
  }

  @Override
  public ImportConfiguration build() {
    built = true;
    return this;
  }

  @Override
  public boolean isKeepOffline() {
    Preconditions.checkState(built, NOT_BUILT_ERROR_MSG);
    return keepOffline;
  }

  @Override
  public boolean isKeepMappings() {
    Preconditions.checkState(built, NOT_BUILT_ERROR_MSG);
    return keepMappingsFile;
  }
}
