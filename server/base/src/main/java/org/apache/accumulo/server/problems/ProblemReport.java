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
package org.apache.accumulo.server.problems;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.accumulo.core.util.Encoding;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;

public class ProblemReport {
  private String tableName;
  private ProblemType problemType;
  private String resource;
  private String exception;
  private String server;
  private long creationTime;

  public ProblemReport(String table, ProblemType problemType, String resource, String server, Throwable e, long creationTime) {
    ArgumentChecker.notNull(table, problemType, resource);
    this.tableName = table;

    this.problemType = problemType;
    this.resource = resource;

    if (e != null) {
      this.exception = e.getMessage();
    }

    if (server == null) {
      try {
        server = InetAddress.getLocalHost().getHostAddress();
      } catch (UnknownHostException e1) {

      }
    }

    this.server = server;
    this.creationTime = creationTime;
  }

  public ProblemReport(String table, ProblemType problemType, String resource, String server, Throwable e) {
    this(table, problemType, resource, server, e, System.currentTimeMillis());
  }

  public ProblemReport(String table, ProblemType problemType, String resource, Throwable e) {
    this(table, problemType, resource, null, e);
  }

  private ProblemReport(String table, ProblemType problemType, String resource, byte enc[]) throws IOException {
    ArgumentChecker.notNull(table, problemType, resource);
    this.tableName = table;
    this.problemType = problemType;
    this.resource = resource;

    decode(enc);
  }

  private byte[] encode() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);

    dos.writeLong(creationTime);

    dos.writeBoolean(server != null);
    if (server != null) {
      dos.writeUTF(server);
    }

    dos.writeBoolean(exception != null);
    if (exception != null) {
      dos.writeUTF(exception);
    }

    dos.close();
    baos.close();

    return baos.toByteArray();
  }

  private void decode(byte enc[]) throws IOException {
    ByteArrayInputStream bais = new ByteArrayInputStream(enc);
    DataInputStream dis = new DataInputStream(bais);

    creationTime = dis.readLong();

    if (dis.readBoolean()) {
      server = dis.readUTF();
    } else {
      server = null;
    }

    if (dis.readBoolean()) {
      exception = dis.readUTF();
    } else {
      exception = null;
    }
  }

  void removeFromMetadataTable() throws Exception {
    Mutation m = new Mutation(new Text("~err_" + tableName));
    m.putDelete(new Text(problemType.name()), new Text(resource));
    MetadataTableUtil.getMetadataTable(SystemCredentials.get()).update(m);
  }

  void saveToMetadataTable() throws Exception {
    Mutation m = new Mutation(new Text("~err_" + tableName));
    m.put(new Text(problemType.name()), new Text(resource), new Value(encode()));
    MetadataTableUtil.getMetadataTable(SystemCredentials.get()).update(m);
  }

  void removeFromZooKeeper() throws Exception {
    removeFromZooKeeper(ZooReaderWriter.getInstance(), HdfsZooInstance.getInstance());
  }

  void removeFromZooKeeper(ZooReaderWriter zoorw, Instance instance) throws IOException, KeeperException, InterruptedException {
    String zpath = getZPath(instance);
    zoorw.recursiveDelete(zpath, NodeMissingPolicy.SKIP);
  }

  void saveToZooKeeper() throws Exception {
    saveToZooKeeper(ZooReaderWriter.getInstance(), HdfsZooInstance.getInstance());
  }

  void saveToZooKeeper(ZooReaderWriter zoorw, Instance instance) throws IOException, KeeperException, InterruptedException {
    zoorw.putPersistentData(getZPath(instance), encode(), NodeExistsPolicy.OVERWRITE);
  }

  private String getZPath(Instance instance) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    dos.writeUTF(getTableName());
    dos.writeUTF(getProblemType().name());
    dos.writeUTF(getResource());
    dos.close();
    baos.close();

    String zpath = ZooUtil.getRoot(instance) + Constants.ZPROBLEMS + "/" + Encoding.encodeAsBase64FileName(new Text(baos.toByteArray()));
    return zpath;
  }

  static ProblemReport decodeZooKeeperEntry(String node) throws Exception {
    return decodeZooKeeperEntry(node, ZooReaderWriter.getInstance(), HdfsZooInstance.getInstance());
  }

  static ProblemReport decodeZooKeeperEntry(String node, ZooReaderWriter zoorw, Instance instance) throws IOException, KeeperException, InterruptedException {
    byte bytes[] = Encoding.decodeBase64FileName(node);

    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    DataInputStream dis = new DataInputStream(bais);

    String tableName = dis.readUTF();
    String problemType = dis.readUTF();
    String resource = dis.readUTF();

    String zpath = ZooUtil.getRoot(instance) + Constants.ZPROBLEMS + "/" + node;
    byte[] enc = zoorw.getData(zpath, null);

    return new ProblemReport(tableName, ProblemType.valueOf(problemType), resource, enc);

  }

  public static ProblemReport decodeMetadataEntry(Entry<Key,Value> entry) throws IOException {
    String tableName = entry.getKey().getRow().toString().substring("~err_".length());
    String problemType = entry.getKey().getColumnFamily().toString();
    String resource = entry.getKey().getColumnQualifier().toString();

    return new ProblemReport(tableName, ProblemType.valueOf(problemType), resource, entry.getValue().get());
  }

  public String getTableName() {
    return tableName;
  }

  public ProblemType getProblemType() {
    return problemType;
  }

  public String getResource() {
    return resource;
  }

  public String getException() {
    return exception;
  }

  public String getServer() {
    return server;
  }

  public long getTime() {
    return creationTime;
  }

  @Override
  public int hashCode() {
    return tableName.hashCode() + problemType.hashCode() + resource.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof ProblemReport) {
      ProblemReport opr = (ProblemReport) o;
      return tableName.equals(opr.tableName) && problemType.equals(opr.problemType) && resource.equals(opr.resource);
    }
    return false;
  }
}
