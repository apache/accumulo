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
import org.apache.accumulo.core.conf.Property;
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
    private boolean samplingEnabled = false;
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

    public Builder withSamplingEnabled() {
      this.samplingEnabled = true;
      return this;
    }

    public Builder withSamplerConfiguration(SamplerConfiguration sc) {
      this.samplerConfig = Optional.ofNullable(sc);
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

  public static final IteratorEnvironment DEFAULT = new Builder().build();

  private final Optional<IteratorScope> scope;
  private final boolean isFullMajorCompaction;
  private final Optional<Authorizations> auths;
  private final boolean isUserCompaction;
  private final Optional<TableId> tableId;
  private final Optional<SamplerConfiguration> samplerConfig;
  private final boolean samplingEnabled;
  private final Optional<ClientServiceEnvironmentImpl> env;

  private ClientIteratorEnvironment(Builder builder) {
    this.scope = builder.scope;
    this.isFullMajorCompaction = builder.isFullMajorCompaction;
    this.auths = builder.auths;
    this.isUserCompaction = builder.isUserCompaction;
    this.tableId = builder.tableId;
    this.samplerConfig = builder.samplerConfig;
    this.env = builder.env;
    this.samplingEnabled = builder.samplingEnabled;
  }

  /**
   * Copy constructor used for enabling sample. Only called from {@link cloneWithSamplingEnabled}.
   */
  private ClientIteratorEnvironment(ClientIteratorEnvironment copy) {
    this.scope = copy.scope;
    this.isFullMajorCompaction = copy.isFullMajorCompaction;
    this.auths = copy.auths;
    this.isUserCompaction = copy.isUserCompaction;
    this.tableId = copy.tableId;
    this.samplerConfig = copy.samplerConfig;
    this.env = copy.env;
    this.samplingEnabled = true;
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
    throw new UnsupportedOperationException("Feature not supported");
  }

  @Override
  public Authorizations getAuthorizations() {
    if (getIteratorScope() != IteratorScope.scan) {
      throw new IllegalStateException("Iterator scope is not scan");
    }
    return auths.orElseThrow();
  }

  @Override
  public IteratorEnvironment cloneWithSamplingEnabled() {
    String samplerClass =
        getPluginEnv().getConfiguration(getTableId()).get(Property.TABLE_SAMPLER.getKey());
    if (samplerClass == null || samplerClass.isBlank()) {
      throw new SampleNotPresentException();
    }
    return new ClientIteratorEnvironment(this);
  }

  @Override
  public boolean isSamplingEnabled() {
    return this.samplingEnabled;
  }

  @Override
  public SamplerConfiguration getSamplerConfiguration() {
    if (!isSamplingEnabled()) {
      return null;
    }
    return samplerConfig.orElseThrow();
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
    return env.orElseThrow();
  }

  @Override
  public TableId getTableId() {
    return this.tableId.orElseThrow();
  }

}
