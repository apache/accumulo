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

import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;

public interface IteratorEnvironment {

  /**
   * @deprecated since 2.0.0. This is a legacy method used for internal backwards compatibility.
   */
  @Deprecated(since = "2.0.0")
  default SortedKeyValueIterator<Key,Value> reserveMapFileReader(String mapFileName)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * @deprecated since 2.0.0. This method was using an unstable non public type. Use
   *             {@link #getPluginEnv()}
   */
  @Deprecated(since = "2.0.0")
  default AccumuloConfiguration getConfig() {
    throw new UnsupportedOperationException();
  }

  /**
   * Return the executed scope of the Iterator. Value will be one of the following:
   * {@link IteratorScope#scan}, {@link IteratorScope#minc}, {@link IteratorScope#majc}
   */
  default IteratorScope getIteratorScope() {
    throw new UnsupportedOperationException();
  }

  /**
   * Return true if the compaction is a full major compaction. Will throw IllegalStateException if
   * {@link #getIteratorScope()} != {@link IteratorScope#majc}.
   */
  default boolean isFullMajorCompaction() {
    throw new UnsupportedOperationException();
  }

  /**
   * @deprecated since 2.0.0. This was an experimental feature and was never tested or documented.
   */
  @Deprecated(since = "2.0.0")
  default void registerSideChannel(SortedKeyValueIterator<Key,Value> iter) {
    throw new UnsupportedOperationException();
  }

  /**
   * Return the Scan Authorizations used in this Iterator. Will throw UnsupportedOperationException
   * if {@link #getIteratorScope()} != {@link IteratorScope#scan}.
   */
  default Authorizations getAuthorizations() {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns a new iterator environment object that can be used to create deep copies over sample
   * data. The new object created will use the current sampling configuration for the table. The
   * existing iterator environment object will not be modified.
   *
   * <p>
   * Since sample data could be created in many different ways, a good practice for an iterator is
   * to verify the sampling configuration is as expected.
   *
   * <pre>
   * <code>
   *   class MyIter implements SortedKeyValueIterator&lt;Key,Value&gt; {
   *     SortedKeyValueIterator&lt;Key,Value&gt; source;
   *     SortedKeyValueIterator&lt;Key,Value&gt; sampleIter;
   *     &#64;Override
   *     void init(SortedKeyValueIterator&lt;Key,Value&gt; source, Map&lt;String,String&gt; options,
   *       IteratorEnvironment env) {
   *       IteratorEnvironment sampleEnv = env.cloneWithSamplingEnabled();
   *       //do some sanity checks on sampling config
   *       validateSamplingConfiguration(sampleEnv.getSamplerConfiguration());
   *       sampleIter = source.deepCopy(sampleEnv);
   *       this.source = source;
   *     }
   *   }
   * </code>
   * </pre>
   *
   * @throws SampleNotPresentException when sampling is not configured for table.
   * @since 1.8.0
   */
  default IteratorEnvironment cloneWithSamplingEnabled() {
    throw new UnsupportedOperationException();
  }

  /**
   * There are at least two conditions under which sampling will be enabled for an environment. One
   * condition is when sampling is enabled for the scan that starts everything. Another possibility
   * is for a deep copy created with an environment created by calling
   * {@link #cloneWithSamplingEnabled()}
   *
   * @return true if sampling is enabled for this environment.
   * @since 1.8.0
   */
  default boolean isSamplingEnabled() {
    throw new UnsupportedOperationException();
  }

  /**
   *
   * @return sampling configuration is sampling is enabled for environment, otherwise returns null.
   * @since 1.8.0
   */
  default SamplerConfiguration getSamplerConfiguration() {
    throw new UnsupportedOperationException();
  }

  /**
   * True if compaction was user initiated.
   *
   * @since 2.0.0
   */
  default boolean isUserCompaction() {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns an object containing information about the server where this iterator was run. To
   * obtain a table configuration, use the following methods:
   *
   * <pre>
   * iterEnv.getServiceEnv().getConfiguration(env.getTableId())
   * </pre>
   *
   * @since 2.0.0
   * @deprecated since 2.1.0. This method was using a non public API type. Use
   *             {@link #getPluginEnv()} instead because it has better stability guarantees.
   */
  @Deprecated(since = "2.1.0")
  default ServiceEnvironment getServiceEnv() {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns an object containing information about the server where this iterator was run. To
   * obtain a table configuration, use the following methods:
   *
   * <pre>
   * iterEnv.getPluginEnv().getConfiguration(env.getTableId())
   * </pre>
   *
   * @since 2.1.0
   */
  default PluginEnvironment getPluginEnv() {
    return getServiceEnv();
  }

  /**
   * Return the table Id associated with this iterator.
   *
   * @since 2.0.0
   */
  default TableId getTableId() {
    throw new UnsupportedOperationException();
  }
}
