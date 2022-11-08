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

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iteratorsImpl.system.MapFileIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class DefaultIteratorEnvironment implements IteratorEnvironment {

  AccumuloConfiguration conf;
  Configuration hadoopConf = new Configuration();

  public DefaultIteratorEnvironment(AccumuloConfiguration conf) {
    this.conf = conf;
  }

  public DefaultIteratorEnvironment() {
    this.conf = DefaultConfiguration.getInstance();
  }

  @Deprecated(since = "2.0.0")
  @Override
  public SortedKeyValueIterator<Key,Value> reserveMapFileReader(String mapFileName)
      throws IOException {
    FileSystem fs = FileSystem.get(hadoopConf);
    return new MapFileIterator(fs, mapFileName, hadoopConf);
  }

  @Override
  public boolean isSamplingEnabled() {
    return false;
  }
}
