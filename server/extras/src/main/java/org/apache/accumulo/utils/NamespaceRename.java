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
package org.apache.accumulo.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.master.thrift.MasterGoalState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.server.Accumulo;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class NamespaceRename {
  
  static final Logger log = LoggerFactory.getLogger(NamespaceRename.class);
  
  static class Opts {
    @Parameter(names = {"--old", "-o"}, required = true)
    String oldName = null;
    @Parameter(names = {"--new", "-n"}, required = true)
    String newName = null;
  }
  
  /**
   * Utility to recovery from a name node restoration at a new location. For example, if you had been using "nn1" and the machine died but you were able to
   * restore the service on a different machine, "nn2" you could rewrite the metadata using
   * <pre>
   * accumulo org.apache.accumulo.server.util.NamespaceRename --old hdfs://nn1:9001 --new hdfs://nn2:9001
   * </pre>
   * @param args
   * @throws Exception 
   */
  public static void main(String[] args) throws Exception {
    Opts opts = new Opts();
    JCommander cmdline = new JCommander(opts);
    cmdline.parse(args);
    log.info("Checking current configuration");
    AccumuloConfiguration configuration = ServerConfiguration.getSiteConfiguration();
    checkConfiguration(opts, configuration);
    Instance instance = HdfsZooInstance.getInstance();
    log.info("Waiting for HDFS and Zookeeper to be ready");
    VolumeManager fs = VolumeManagerImpl.get();
    Accumulo.waitForZookeeperAndHdfs(fs);
    log.info("Putting servers in SAFE_MODE");
    ZooReaderWriter zoo = ZooReaderWriter.getInstance();
    zoo.putPersistentData(ZooUtil.getRoot(HdfsZooInstance.getInstance()) + Constants.ZMASTER_GOAL_STATE, MasterGoalState.SAFE_MODE.toString().getBytes(), NodeExistsPolicy.OVERWRITE);
    log.info("Updating root table write-ahead logs");
    updateZookeeper(opts, instance, zoo);
    log.info("Updating file references in the root table");
    updateMetaTable(opts, instance, RootTable.NAME);
    log.info("Updating file references in the metadata table");
    updateMetaTable(opts, instance, MetadataTable.NAME);
    zoo.putPersistentData(ZooUtil.getRoot(HdfsZooInstance.getInstance()) + Constants.ZMASTER_GOAL_STATE, MasterGoalState.NORMAL.toString().getBytes(), NodeExistsPolicy.OVERWRITE);
    log.info("Namespace " + opts.oldName + " has been renamed " + opts.newName);
  }
  
  static final ColumnFQ DIRECTORY_COLUMN = MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN;

  private static void updateMetaTable(Opts opts, Instance instance, String tableName) throws Exception,
      MutationsRejectedException {
    log.info("Waiting for " + tableName + " to come online");
    Connector conn = getConnector(instance);
    Scanner scanner = conn.createScanner(tableName, Authorizations.EMPTY);
    scanner.fetchColumnFamily(MetadataSchema.TabletsSection.LogColumnFamily.NAME);
    scanner.fetchColumnFamily(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME);
    DIRECTORY_COLUMN.fetch(scanner);
    scanner.iterator().hasNext();
    log.info(tableName + " is online");
    BatchWriter bw = conn.createBatchWriter(tableName,  new BatchWriterConfig());
    for (Entry<Key,Value> entry : scanner) {
      Key key = entry.getKey();
      Mutation m = new Mutation(key.getRow());
      if (DIRECTORY_COLUMN.equals(key.getColumnFamily(), key.getColumnQualifier())) {
        m.putDelete(key.getColumnFamily(), key.getColumnQualifier(), key.getTimestamp());
        String newName = rename(entry.getValue().toString(), opts);
        m.put(key.getColumnFamily(), key.getColumnQualifier(), new Value(newName.getBytes()));
        bw.addMutation(m);
      } else if (key.getColumnFamily().equals(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME)) {
        m.putDelete(key.getColumnFamily(), key.getColumnQualifier(), key.getTimestamp());
        String newName = rename(key.getColumnQualifier().toString(), opts);
        m.put(key.getColumnFamily(), new Text(newName), entry.getValue());
        bw.addMutation(m);
      } else if (key.getColumnFamily().equals(MetadataSchema.TabletsSection.LogColumnFamily.NAME)) {
        m.putDelete(key.getColumnFamily(), key.getColumnQualifier(), key.getTimestamp());
        LogEntry update = convert(LogEntry.fromKeyValue(entry.getKey(), entry.getValue()), opts);
        m.put(update.getColumnFamily(), update.getColumnQualifier(), update.getValue());
        bw.addMutation(m);
      }
    }
    bw.close();
  }

  static private LogEntry convert(LogEntry entry, Opts opts) {
    entry.filename = rename(entry.filename, opts);
    List<String> logSet = new ArrayList<String>();
    for (String log : entry.logSet) {
      logSet.add(rename(log, opts));
    }
    entry.logSet = logSet;
    return entry;
  }
  
  private static Connector getConnector(Instance instance) throws AccumuloException, AccumuloSecurityException {
    return instance.getConnector(SystemCredentials.get().getPrincipal(), SystemCredentials.get().getToken());
  }

  private static void updateZookeeper(Opts opts, Instance instance, ZooReaderWriter zoo) throws KeeperException, InterruptedException,
      IOException {
    String root = ZooUtil.getRoot(instance);
    String rootTabletLocation = root + RootTable.ZROOT_TABLET_WALOGS;
    for (String walogName : zoo.getChildren(rootTabletLocation)) {
      LogEntry entry = new LogEntry();
      String logZPath = rootTabletLocation + "/" + walogName;
      byte[] data = zoo.getData(logZPath, null);
      entry.fromBytes(data);
      entry = convert(entry, opts);
      zoo.putPersistentData(logZPath, entry.toBytes(), NodeExistsPolicy.OVERWRITE);
    }
    String dirPath = root + RootTable.ZROOT_TABLET_PATH;
    byte[] dir = zoo.getData(dirPath, null);
    String newDir = rename(new String(dir), opts);
    zoo.putPersistentData(dirPath, newDir.getBytes(), NodeExistsPolicy.OVERWRITE);
  }

  private static String rename(String filename, Opts opts) {
    if (filename.startsWith(opts.oldName))
      return opts.newName + filename.substring(opts.oldName.length(), filename.length());
    return filename;
  }

  private static void checkConfiguration(Opts opts, AccumuloConfiguration configuration) throws IOException {
    if (opts.oldName.endsWith("/"))
      throw new RuntimeException(opts.oldName + " ends with a slash, do not include it");
    if (opts.newName.endsWith("/"))
      throw new RuntimeException(opts.newName + " ends with a slash, do not include it");
    String volumes = configuration.get(Property.INSTANCE_VOLUMES);
    if (volumes != null && !volumes.isEmpty()) {
      Set<String> volumeSet = new HashSet<String>(Arrays.asList(volumes.split(",")));
      if (volumeSet.contains(opts.oldName))
        throw new RuntimeException(Property.INSTANCE_VOLUMES.getKey() + " is set to " + volumes + " which still contains the old name " + opts.oldName);
      if (!volumeSet.contains(opts.newName))
        throw new RuntimeException(Property.INSTANCE_VOLUMES.getKey() + " is set to " + volumes + " which does not contain the new name " + opts.oldName);
      return;
    } else {
      String uri = configuration.get(Property.INSTANCE_DFS_URI);
      if (uri != null && !uri.isEmpty()) {
        if (!uri.startsWith(opts.newName))
          throw new RuntimeException(Property.INSTANCE_DFS_DIR.getKey() + " is set to " + uri + " which is not in " + opts.newName);
        return;
      }
    }
    FileSystem fs = FileSystem.get(CachedConfiguration.getInstance());
    if (!fs.getUri().toString().equals(opts.newName))
      throw new RuntimeException("Default filesystem is " + fs.getUri() + " and the new name is " + opts.newName + ". Update your hadoop dfs configuration.");
  }
  
}
