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
package org.apache.accumulo.tserver.log;

import static org.apache.accumulo.tserver.logger.LogEvents.DEFINE_TABLET;
import static org.apache.accumulo.tserver.logger.LogEvents.OPEN;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeMap;

import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.spi.crypto.GenericCryptoServiceFactory;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.log.SortedLogState;
import org.apache.accumulo.tserver.WithTestNames;
import org.apache.accumulo.tserver.logger.LogFileKey;
import org.apache.accumulo.tserver.logger.LogFileValue;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths not set by user input")
public class RecoveryLogsIteratorTest extends WithTestNames {

  private VolumeManager fs;
  private File workDir;
  static final KeyExtent extent = new KeyExtent(TableId.of("table"), null, null);
  static ServerContext context;
  static LogSorter logSorter;

  @TempDir
  private static File tempDir;

  @BeforeEach
  public void setUp() throws Exception {
    context = createMock(ServerContext.class);

    workDir = new File(tempDir, testName());
    String path = workDir.getAbsolutePath();
    fs = VolumeManagerImpl.getLocalForTesting(path);
    expect(context.getCryptoFactory()).andReturn(new GenericCryptoServiceFactory()).anyTimes();
    expect(context.getVolumeManager()).andReturn(fs).anyTimes();
    expect(context.getConfiguration()).andReturn(DefaultConfiguration.getInstance()).anyTimes();
    replay(context);

    logSorter = new LogSorter(context, DefaultConfiguration.getInstance());
  }

  @AfterEach
  public void tearDown() throws Exception {
    fs.close();
  }

  static class KeyValue implements Comparable<KeyValue> {
    public final LogFileKey key;
    public final LogFileValue value;

    KeyValue() {
      key = new LogFileKey();
      value = new LogFileValue();
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(key) + Objects.hashCode(value);
    }

    @Override
    public boolean equals(Object obj) {
      return this == obj || (obj instanceof KeyValue && 0 == compareTo((KeyValue) obj));
    }

    @Override
    public int compareTo(KeyValue o) {
      return key.compareTo(o.key);
    }
  }

  @Test
  public void testSimpleRLI() throws IOException {
    KeyValue keyValue = new KeyValue();
    keyValue.key.event = DEFINE_TABLET;
    keyValue.key.seq = 0;
    keyValue.key.tabletId = 1;
    keyValue.key.tablet = extent;

    KeyValue[] keyValues = {keyValue};

    Map<String,KeyValue[]> logs = new TreeMap<>();
    logs.put("keyValues", keyValues);

    ArrayList<Path> dirs = new ArrayList<>();

    createRecoveryDir(logs, dirs, true);

    try (RecoveryLogsIterator rli = new RecoveryLogsIterator(context, dirs, null, null, false)) {
      while (rli.hasNext()) {
        Entry<LogFileKey,LogFileValue> entry = rli.next();
        assertEquals(1, entry.getKey().tabletId, "TabletId does not match");
        assertEquals(DEFINE_TABLET, entry.getKey().event, "Event does not match");
      }
    }
  }

  @Test
  public void testFinishMarker() throws IOException {
    KeyValue keyValue = new KeyValue();
    keyValue.key.event = DEFINE_TABLET;
    keyValue.key.seq = 0;
    keyValue.key.tabletId = 1;
    keyValue.key.tablet = extent;

    KeyValue[] keyValues = {keyValue};

    Map<String,KeyValue[]> logs = new TreeMap<>();
    logs.put("keyValues", keyValues);

    ArrayList<Path> dirs = new ArrayList<>();

    createRecoveryDir(logs, dirs, false);

    assertThrows(IOException.class,
        () -> new RecoveryLogsIterator(context, dirs, null, null, false),
        "Finish marker should not be found");
  }

  @Test
  public void testSingleFile() throws IOException {
    String destPath = workDir + "/test.rf";
    fs.create(new Path(destPath));

    assertThrows(
        IOException.class, () -> new RecoveryLogsIterator(context,
            Collections.singletonList(new Path(destPath)), null, null, false),
        "Finish marker should not be found for a single file.");
  }

  @Test
  public void testCheckFirstKeyFailed() throws IOException {
    KeyValue keyValue = new KeyValue();
    keyValue.key.event = DEFINE_TABLET;
    keyValue.key.seq = 0;
    keyValue.key.tabletId = 1;
    keyValue.key.tablet = extent;

    KeyValue[] keyValues = {keyValue};

    Map<String,KeyValue[]> logs = new TreeMap<>();
    logs.put("keyValues", keyValues);

    ArrayList<Path> dirs = new ArrayList<>();

    createRecoveryDir(logs, dirs, true);

    assertThrows(IllegalStateException.class,
        () -> new RecoveryLogsIterator(context, dirs, null, null, true),
        "First log entry is not OPEN so exception should be thrown.");
  }

  @Test
  public void testCheckFirstKeyPass() throws IOException {
    KeyValue keyValue1 = new KeyValue();
    keyValue1.key.event = OPEN;
    keyValue1.key.seq = 0;
    keyValue1.key.tabletId = -1;
    keyValue1.key.tserverSession = "1";

    KeyValue keyValue2 = new KeyValue();
    keyValue2.key.event = DEFINE_TABLET;
    keyValue2.key.seq = 0;
    keyValue2.key.tabletId = 1;
    keyValue2.key.tablet = extent;

    KeyValue[] keyValues = {keyValue1, keyValue2};

    Map<String,KeyValue[]> logs = new TreeMap<>();
    logs.put("keyValues", keyValues);

    ArrayList<Path> dirs = new ArrayList<>();

    createRecoveryDir(logs, dirs, true);

    try (RecoveryLogsIterator rli = new RecoveryLogsIterator(context, dirs, null, null, true)) {
      while (rli.hasNext()) {
        Entry<LogFileKey,LogFileValue> entry = rli.next();
        assertNotNull(entry.getKey());
      }
    }
  }

  private void createRecoveryDir(Map<String,KeyValue[]> logs, ArrayList<Path> dirs,
      boolean FinishMarker) throws IOException {

    for (Entry<String,KeyValue[]> entry : logs.entrySet()) {
      String destPath = workDir + "/dir";
      FileSystem ns = fs.getFileSystemByPath(new Path(destPath));

      // convert test object to Pairs for LogSorter.
      List<Pair<LogFileKey,LogFileValue>> buffer = new ArrayList<>();
      for (KeyValue pair : entry.getValue()) {
        buffer.add(new Pair<>(pair.key, pair.value));
      }
      logSorter.writeBuffer(destPath, buffer, 0);

      if (FinishMarker) {
        ns.create(SortedLogState.getFinishedMarkerPath(destPath));
      }

      dirs.add(new Path(destPath));
    }
  }
}
