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
package org.apache.accumulo.core.file;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.file.blockfile.impl.CacheProvider;
import org.apache.accumulo.core.iteratorsImpl.system.InterruptibleIterator;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;

public interface FileSKVIterator extends InterruptibleIterator, AutoCloseable {
  Key getFirstKey() throws IOException;

  Key getLastKey() throws IOException;

  DataInputStream getMetaStore(String name) throws IOException, NoSuchMetaStoreException;

  /**
   * Returns an estimate of the number of entries that overlap the given extent. This is an estimate
   * because the extent may or may not entirely overlap with each of the index entries included in
   * the count. Will never underestimate but may overestimate.
   *
   * @param extent the key extent
   * @return the estimate
   */
  long estimateOverlappingEntries(KeyExtent extent) throws IOException;

  FileSKVIterator getSample(SamplerConfigurationImpl sampleConfig);

  void closeDeepCopies() throws IOException;

  void setCacheProvider(CacheProvider cacheProvider);

  @Override
  void close() throws IOException;
}
