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
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link HadoopLogCloser} recovers leases in DistributedFileSystem implementations and does nothing
 * in local FileSystem implementations. For other implementations {@link HadoopLogCloser} throws an
 * exception. However, Accumulo could be used with other Hadoop compatible FileSystem
 * implementations that do not support lease recovery and we should not throw an exception in this
 * case. This LogCloser implementation supports that case.
 *
 * WARNING: USE AT YOUR OWN RISK! When using this class as the LogCloser, it's possible that WALog
 * recovery will not work if the file system implementation needs some type of recovery to occur for
 * files that were being written to, but not properly closed.
 *
 * To use this class, set the Property {@link Property#MANAGER_WAL_CLOSER_IMPLEMENTATION} to the
 * full name of this class.
 */
public class NoOpLogCloser implements LogCloser {

  private static final Logger LOG = LoggerFactory.getLogger(NoOpLogCloser.class);

  public NoOpLogCloser() {
    LOG.warn("This log closer makes no attempt at recovering write ahead logs so that they can be "
        + "properly closed. WALog recovery is not guaranteed to work when using this class.");
  }

  @Override
  public long close(AccumuloConfiguration conf, Configuration hadoopConf, VolumeManager fs,
      Path path) throws IOException {
    return 0;
  }

}
