/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.tserver.log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.tserver.logger.LogFileKey;
import org.apache.accumulo.tserver.logger.LogFileValue;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;

/**
 * Iterates over multiple sorted recovery logs merging them into a single sorted stream.
 */
public class RecoveryLogsIterator implements CloseableIterator<Entry<LogFileKey,LogFileValue>> {

  private static final Logger LOG = LoggerFactory.getLogger(RecoveryLogsIterator.class);

  List<CloseableIterator<Entry<LogFileKey,LogFileValue>>> iterators;
  private UnmodifiableIterator<Entry<LogFileKey,LogFileValue>> iter;

  /**
   * Iterates only over keys in the range [start,end].
   */
  RecoveryLogsIterator(VolumeManager fs, List<Path> recoveryLogPaths, LogFileKey start,
      LogFileKey end) throws IOException {

    iterators = new ArrayList<>(recoveryLogPaths.size());

    try {
      for (Path log : recoveryLogPaths) {
        LOG.debug("Opening recovery log {}", log.getName());
        RecoveryLogReader rlr = new RecoveryLogReader(fs, log, start, end);
        if (rlr.hasNext()) {
          LOG.debug("Write ahead log {} has data in range {} {}", log.getName(), start, end);
          iterators.add(rlr);
        } else {
          LOG.debug("Write ahead log {} has no data in range {} {}", log.getName(), start, end);
          rlr.close();
        }
      }

      iter = Iterators.mergeSorted(iterators, (o1, o2) -> o1.getKey().compareTo(o2.getKey()));

    } catch (RuntimeException | IOException e) {
      try {
        close();
      } catch (Exception e2) {
        e.addSuppressed(e2);
      }
      throw e;
    }
  }

  @Override
  public boolean hasNext() {
    return iter.hasNext();
  }

  @Override
  public Entry<LogFileKey,LogFileValue> next() {
    return iter.next();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("remove");
  }

  @Override
  public void close() {
    for (CloseableIterator<?> reader : iterators) {
      try {
        reader.close();
      } catch (IOException e) {
        LOG.debug("Failed to close reader", e);
      }
    }
  }
}
