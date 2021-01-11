/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.tserver.log;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LogColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.log.SortedLogState;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.google.common.primitives.Bytes;

public class CorruptWalReplacer {

  private static final Logger log = LoggerFactory.getLogger(CorruptWalReplacer.class);
  private static final byte[] EMPTY_WAL_CONTENT = Bytes
      .concat("--- Log File Header (v2) ---".getBytes(UTF_8), new byte[] {0, 0, 0, 0});

  private Connector connector;
  private String quarantineDir;
  private String workDir;

  public CorruptWalReplacer(String instanceName, String zooKeepers, String user, String password,
      String quarantineDir, String workDir) throws AccumuloSecurityException, AccumuloException {
    ZooKeeperInstance instance = new ZooKeeperInstance(instanceName, zooKeepers);
    this.connector = instance.getConnector(user, new PasswordToken(password));
    this.quarantineDir = quarantineDir;
    this.workDir = workDir;
  }

  public void run() throws Exception {
    VolumeManager fs = VolumeManagerImpl.get();
    Path quarantineDir = new Path(this.quarantineDir);
    Path workDir = new Path(this.workDir);

    for (KeyExtent extent : getExtents()) {
      log.info("Checking logs for extent {}", extent);
      List<Path> logs = getLogs(extent);

      if (logs.isEmpty()) {
        log.info("No logs found for key {}", extent);
        continue;
      }

      // Clean up the working directory, if it exists
      ArrayList<Path> dirs = new ArrayList<>();
      if (fs.exists(workDir)) {
        log.info("Deleting {}", workDir);
        fs.deleteRecursively(workDir);
      }

      try {
        // Map containing the name of the file to the full path
        Map<String,Path> nameToSource = new HashMap<>();
        for (Path path : logs) {
          Path destPath = new Path(workDir, path.getName());

          // Run the log sorter task to prepare for recovery
          LogSorter.LogSorterTask task = new LogSorter.LogSorterTask(fs,
              AccumuloConfiguration.getDefaultConfiguration());

          log.info("Invoking sort from path {} to path {}", path, destPath);
          task.sort(path.getName(), path, destPath.toString());

          log.info("Creating finished marker at {}",
              SortedLogState.getFinishedMarkerPath(destPath));
          fs.create(SortedLogState.getFinishedMarkerPath(destPath)).close();
          dirs.add(destPath);
          nameToSource.put(path.getName(), path);
          log.info("Mapped {} to {}", path.getName(), path);
        }

        try {
          log.info("Starting 'recovery' process");
          SortedLogRecovery recovery = new SortedLogRecovery(fs);
          CaptureMutations capture = new CaptureMutations();
          recovery.recover(extent, dirs, new HashSet<String>(), capture);
          log.info("Logs are good; key {} has {} mutations", extent, capture.getNumMutations());
        } catch (LogRecoveryException e) {
          // An exception occurred; one of the logs was unrecoverable
          log.error(e.toString());

          // Determine the path to quarantine the file
          Path quarantinePath = new Path(String.format("%s/%s/%s/%s", quarantineDir,
              URLEncoder.encode(extent.toString(), "UTF-8"), System.currentTimeMillis(),
              e.getLog()));
          Path source = nameToSource.get(e.getLog());

          if (source == null) {
            throw new RuntimeException("Failed to quarantine; source is null");
          }

          // Create the quarantine path's parent directory
          if (!fs.mkdirs(quarantinePath.getParent())) {
            log.error("Failed to mkdirs {}; not creating empty WAL", quarantinePath.getParent());
            continue;
          }

          // Move the unrecoverable WAL to quarantine
          log.info("Moving {} to {}", source, quarantinePath);
          if (!fs.rename(source, quarantinePath)) {
            log.error("Failed to rename file into quarantine directory; not creating empty WAL");
            continue;
          }

          // Write out the empty log
          log.info("Writing empty WAL to {}", source);
          FSDataOutputStream out = fs.create(source);
          out.write(EMPTY_WAL_CONTENT);
          out.close();

          // Delete the old recovery path
          Path oldRecoveryPath = new Path("/accumulo/recovery", e.getLog());
          if (fs.exists(oldRecoveryPath)) {
            log.info("Deleting old recovery path {}", oldRecoveryPath);
            if (!fs.deleteRecursively(oldRecoveryPath)) {
              log.error("Failed to delete path {}", oldRecoveryPath);
            }
          } else {
            log.warn("Old recovery path {} does not exist");
          }
        }
      } finally {
        // Clean up the working directory on exit
        if (fs.exists(workDir)) {
          log.info("Deleting {}", workDir);
          fs.deleteRecursively(workDir);
        }
      }
    }
    log.info("Done");
  }

  private Set<KeyExtent> getExtents() throws TableNotFoundException {
    log.info("Searching for 'log' entries in accumulo.metadata for key extents to check");

    try (Scanner scanner = connector.createScanner("accumulo.metadata", new Authorizations())) {
      scanner.fetchColumnFamily(new Text("log"));
      scanner.fetchColumnFamily(new Text("~tab"));

      Set<KeyExtent> extents = new TreeSet<>();
      RowIterator rowIter = new RowIterator(scanner);
      while (rowIter.hasNext()) {
        Iterator<Map.Entry<Key,Value>> row = rowIter.next();
        KeyExtent extent = null;
        boolean hasLogs = false;
        Text rowKey = null;

        while (row.hasNext()) {
          Map.Entry<Key,Value> entry = row.next();
          Key key = entry.getKey();
          rowKey = entry.getKey().getRow();

          if (LogColumnFamily.NAME.equals(key.getColumnFamily())) {
            hasLogs = true;
          }

          if (TabletColumnFamily.PREV_ROW_COLUMN.hasColumns(key)) {
            extent = new KeyExtent(key.getRow(), entry.getValue());
          }
        }

        if (hasLogs) {
          if (extent != null) {
            log.info("Found {} with logs", extent);
            extents.add(extent);
          } else {
            throw new IllegalStateException("No extent found but there are logs for row " + rowKey);
          }
        }
      }

      log.info("Found {} keys to check", extents.size());
      return extents;
    }
  }

  private List<Path> getLogs(KeyExtent extent) throws TableNotFoundException {
    log.info("Searching accumulo.metadata for extent: {}", extent);

    try (Scanner scanner = connector.createScanner("accumulo.metadata", new Authorizations())) {
      log.info("Fetching column family {}", LogColumnFamily.NAME);
      scanner.fetchColumnFamily(LogColumnFamily.NAME);
      log.info("Scanning range {}", extent.toMetadataRange());
      scanner.setRange(extent.toMetadataRange());

      List<Path> paths = new ArrayList<>();
      for (Map.Entry<Key,Value> entry : scanner) {
        Path path = new Path(new String(entry.getValue().get(), UTF_8));
        log.info("Found path {}", path);
        paths.add(path);
      }

      log.info("Found {} WALs to check", paths.size());
      return paths;
    }
  }

  static class Opts extends Help {
    @Parameter(names = "-i", required = true, description = "Accumulo instance name")
    String instanceName;

    @Parameter(names = "-z", required = true, description = "Accumulo ZooKeeper connect string")
    String zooKeepers;

    @Parameter(names = "-u", required = true, description = "Accumulo user name")
    String user;

    @Parameter(names = "-p", required = true, description = "Accumulo password")
    String password;

    @Parameter(names = "-q", required = true, description = "WAL quarantine directory")
    String quarantineDir;

    @Parameter(names = "-w", required = true,
        description = "Working directory for the tool; any existing contents will be deleted")
    String workDir;
  }

  public static void main(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs(CorruptWalReplacer.class.getName(), args);

    CorruptWalReplacer corruptWalReplacer = new CorruptWalReplacer(opts.instanceName,
        opts.zooKeepers, opts.user, opts.password, opts.quarantineDir, opts.workDir);

    corruptWalReplacer.run();
  }

  private static class CaptureMutations implements MutationReceiver {
    public long numMutations = 0;

    @Override
    public void receive(Mutation m) {
      numMutations++;
    }

    public long getNumMutations() {
      return numMutations;
    }
  }
}
