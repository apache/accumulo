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
package org.apache.accumulo.server.iterators;

import java.util.ArrayList;
import java.util.Optional;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.ClientIteratorEnvironment;
import org.apache.accumulo.core.iteratorsImpl.system.MultiIterator;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServiceEnvironmentImpl;

/**
 * IteratorEnvironment used on the server side that has a little different handling for Sampling and
 * provides access to the ServerContext and top level iterators.
 */
public class SystemIteratorEnvironmentImpl extends ClientIteratorEnvironment
    implements SystemIteratorEnvironment {

  public static class Builder extends ClientIteratorEnvironment.Builder {

    private final ServerContext ctx;
    private ArrayList<SortedKeyValueIterator<Key,Value>> topLevelIterators = new ArrayList<>();

    public Builder(ServerContext ctx) {
      this.ctx = ctx;
      this.env = Optional.of(new ServiceEnvironmentImpl(ctx));
    }

    public Builder
        withTopLevelIterators(ArrayList<SortedKeyValueIterator<Key,Value>> topLevelIterators) {
      this.topLevelIterators = topLevelIterators;
      return this;
    }

    @Override
    public Builder withClient(AccumuloClient client) {
      // Does nothing, this was set in constructor
      return this;
    }

    @Override
    public SystemIteratorEnvironmentImpl build() {
      return new SystemIteratorEnvironmentImpl(this);
    }

  }

  private final ServerContext ctx;
  private final ArrayList<SortedKeyValueIterator<Key,Value>> topLevelIterators;

  protected SystemIteratorEnvironmentImpl(SystemIteratorEnvironmentImpl.Builder builder) {
    super(builder);
    this.ctx = builder.ctx;
    this.topLevelIterators = builder.topLevelIterators;
  }

  private SystemIteratorEnvironmentImpl(SystemIteratorEnvironmentImpl copy) {
    super(copy);
    this.ctx = copy.ctx;
    this.topLevelIterators = copy.topLevelIterators;
  }

  @Override
  public SamplerConfiguration getSamplerConfiguration() {
    // Server-side we take into account the table configuration for sampling
    if (samplerConfig == null || samplerConfig.isEmpty()) {
      // only create this once so that it stays the same, even if config changes
      SamplerConfigurationImpl sci =
          SamplerConfigurationImpl.newSamplerConfig(ctx.getTableConfiguration(getTableId()));
      if (sci == null) {
        return null;
      }
      samplerConfig = Optional.of(sci.toSamplerConfiguration());
    }
    return samplerConfig.orElse(null);
  }

  @Override
  public IteratorEnvironment cloneWithSamplingEnabled() {
    if (!getIteratorScope().equals(IteratorScope.scan)) {
      throw new UnsupportedOperationException();
    }
    if (getSamplerConfiguration() == null) {
      throw new SampleNotPresentException();
    }
    return new SystemIteratorEnvironmentImpl(this);
  }

  @Override
  public ServerContext getServerContext() {
    return this.ctx;
  }

  @Override
  public SortedKeyValueIterator<Key,Value>
      getTopLevelIterator(SortedKeyValueIterator<Key,Value> iter) {
    if (topLevelIterators.isEmpty()) {
      return iter;
    }
    ArrayList<SortedKeyValueIterator<Key,Value>> allIters = new ArrayList<>(topLevelIterators);
    allIters.add(iter);
    return new MultiIterator(allIters);
  }

}
