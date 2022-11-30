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
package org.apache.accumulo.server.manager.recovery;

import java.io.IOException;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * Object that is used by the RecoveryManager to properly close WALogs that were being written to
 * and were not properly closed (TabletServer died, for example).
 */
public interface LogCloser {

  /**
   * Attempts to properly close a WALog
   *
   * @param conf AccumuloConfiguration
   * @param hadoopConf Hadoop configuration
   * @param fs VolumeManager
   * @param path WALog file path
   * @return amount of time to wait before retrying, 0 if succeeded
   * @throws IOException exception closing walog
   */
  long close(AccumuloConfiguration conf, Configuration hadoopConf, VolumeManager fs, Path path)
      throws IOException;
}
