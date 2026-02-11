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

import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.security.Authorizations;

public interface IteratorEnvironment {

  /**
   * @return the executed scope of the Iterator. Value will be one of the following:
   *         {@link IteratorScope#scan}, {@link IteratorScope#minc}, {@link IteratorScope#majc}
   */
  IteratorScope getIteratorScope();

  /**
   * @return true if the compaction is a full major compaction.
   * @throws IllegalStateException if {@link #getIteratorScope()} != {@link IteratorScope#majc}.
   */
  boolean isFullMajorCompaction();

  /**
   * @return the Scan Authorizations used in this Iterator.
   * @throws UnsupportedOperationException if {@link #getIteratorScope()} !=
   *         {@link IteratorScope#scan}.
   */
  Authorizations getAuthorizations();

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
  IteratorEnvironment cloneWithSamplingEnabled();

  /**
   * There are at least two conditions under which sampling will be enabled for an environment. One
   * condition is when sampling is enabled for the scan that starts everything. Another possibility
   * is for a deep copy created with an environment created by calling
   * {@link #cloneWithSamplingEnabled()}
   *
   * @return true if sampling is enabled for this environment.
   * @since 1.8.0
   */
  boolean isSamplingEnabled();

  /**
   *
   * @return sampling configuration if sampling is enabled for environment, otherwise returns null.
   * @since 1.8.0
   */
  SamplerConfiguration getSamplerConfiguration();

  /**
   * @return true if compaction was user initiated.
   *
   * @since 2.0.0
   */
  boolean isUserCompaction();

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
  PluginEnvironment getPluginEnv();

  /**
   * @return the table id associated with this iterator or null if there is no table id associated
   *         (e.g., iterator for RFileScanner)
   *
   * @since 2.0.0
   */
  TableId getTableId();

  /**
   * Return whether or not the server is running low on memory
   *
   * @return true if server is running low on memory
   * @since 3.0.0
   */
  boolean isRunningLowOnMemory();
}
