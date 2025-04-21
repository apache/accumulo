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
package org.apache.accumulo.core.iterators;

import java.io.IOException;
import java.util.Optional;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.ClientServiceEnvironmentImpl;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iteratorsImpl.system.MapFileIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class ClientIteratorEnvironment implements IteratorEnvironment {

  public static class Builder {

    private Optional<IteratorScope> scope = Optional.empty();
    private boolean isFullMajorCompaction = false;
    private Optional<Authorizations> auths = Optional.empty();
    private boolean isUserCompaction = false;
    private Optional<TableId> tableId = Optional.empty();
    private Optional<SamplerConfiguration> samplerConfig = Optional.empty();
    protected Optional<ClientServiceEnvironmentImpl> env = Optional.empty();

    public Builder withScope(IteratorScope scope) {
      this.scope = Optional.of(scope);
      return this;
    }

    public Builder isFullMajorCompaction() {
      this.isFullMajorCompaction = true;
      return this;
    }

    public Builder withAuthorizations(Authorizations auths) {
      this.auths = Optional.of(auths);
      return this;
    }

    public Builder isUserCompaction() {
      this.isUserCompaction = true;
      return this;
    }

    public Builder withTableId(TableId tableId) {
      this.tableId = Optional.of(tableId);
      return this;
    }

    public Builder withSamplerConfiguration(SamplerConfiguration sc) {
      this.samplerConfig = Optional.of(sc);
      return this;
    }

    public Builder withClient(AccumuloClient client) {
      this.env = Optional.of(new ClientServiceEnvironmentImpl((ClientContext) client));
      return this;
    }

    public ClientIteratorEnvironment build() {
      return new ClientIteratorEnvironment(this);
    }

  }

  private static final UnsupportedOperationException UOE =
      new UnsupportedOperationException("Feature not supported");

  public static final IteratorEnvironment DEFAULT = new Builder().build();

  private final Optional<IteratorScope> scope;
  private final boolean isFullMajorCompaction;
  private final Optional<Authorizations> auths;
  private final boolean isUserCompaction;
  private final Optional<TableId> tableId;
  private final Optional<SamplerConfiguration> samplerConfig;
  private final Optional<ClientServiceEnvironmentImpl> env;

  private ClientIteratorEnvironment(Builder builder) {
    this.scope = builder.scope;
    this.isFullMajorCompaction = builder.isFullMajorCompaction;
    this.auths = builder.auths;
    this.isUserCompaction = builder.isUserCompaction;
    this.tableId = builder.tableId;
    this.samplerConfig = builder.samplerConfig;
    this.env = builder.env;
  }

  private ClientIteratorEnvironment(ClientIteratorEnvironment copy) {
    this.scope = copy.scope.isPresent() ? Optional.of(copy.scope.orElseThrow()) : Optional.empty();
    this.isFullMajorCompaction = copy.isFullMajorCompaction;
    this.auths = copy.auths.isPresent() ? Optional.of(copy.auths.orElseThrow()) : Optional.empty();
    this.isUserCompaction = copy.isUserCompaction;
    this.tableId =
        copy.tableId.isPresent() ? Optional.of(copy.tableId.orElseThrow()) : Optional.empty();
    this.samplerConfig = copy.samplerConfig.isPresent()
        ? Optional.of(copy.samplerConfig.orElseThrow()) : Optional.empty();
    this.env = copy.env.isPresent() ? Optional.of(copy.env.orElseThrow()) : Optional.empty();
  }

  @Override
  @Deprecated(since = "2.0.0")
  public SortedKeyValueIterator<Key,Value> reserveMapFileReader(String mapFileName)
      throws IOException {
    Configuration hadoopConf = new Configuration();
    FileSystem fs = FileSystem.get(hadoopConf);
    return new MapFileIterator(fs, mapFileName, hadoopConf);
  }

  @Override
  public IteratorScope getIteratorScope() {
    return scope.orElseThrow();
  }

  @Override
  public boolean isFullMajorCompaction() {
    if (getIteratorScope() != IteratorScope.majc) {
      throw new IllegalStateException("Iterator scope is not majc");
    }
    return isFullMajorCompaction;
  }

  @Override
  @Deprecated(since = "2.0.0")
  public void registerSideChannel(SortedKeyValueIterator<Key,Value> iter) {
    throw UOE;
  }

  @Override
  public Authorizations getAuthorizations() {
    if (getIteratorScope() != IteratorScope.scan) {
      throw new IllegalStateException("Iterator scope is not majc");
    }
    return auths.orElseThrow();
  }

  @Override
  public IteratorEnvironment cloneWithSamplingEnabled() {
    if (!isSamplingEnabled()) {
      throw new SampleNotPresentException();
    }
    return new ClientIteratorEnvironment(this);
  }

  @Override
  public boolean isSamplingEnabled() {
    return this.samplerConfig.isPresent();
  }

  @Override
  public SamplerConfiguration getSamplerConfiguration() {
    return samplerConfig.orElse(null);
  }

  @Override
  public boolean isUserCompaction() {
    if (getIteratorScope() == IteratorScope.scan) {
      throw new IllegalStateException(
          "scan iterator scope is incompatible with a possible user compaction");
    }
    return this.isUserCompaction;
  }

  @Override
  @Deprecated(since = "2.1.0")
  public ServiceEnvironment getServiceEnv() {
    return env.orElseThrow();
  }

  @Override
  public PluginEnvironment getPluginEnv() {
    return getServiceEnv();
  }

  @Override
  public TableId getTableId() {
    return this.tableId.orElseThrow();
  }

}
