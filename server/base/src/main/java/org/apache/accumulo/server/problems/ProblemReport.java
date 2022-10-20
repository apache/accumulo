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
package org.apache.accumulo.server.problems;

import static java.util.Objects.requireNonNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.core.util.Encoding;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;

public class ProblemReport {
  private TableId tableId;
  private ProblemType problemType;
  private String resource;
  private String exception;
  private String server;
  private long creationTime;

  public ProblemReport(TableId tableId, ProblemType problemType, String resource, String server,
      Throwable e, long creationTime) {
    requireNonNull(tableId, "tableId is null");
    requireNonNull(problemType, "problemType is null");
    requireNonNull(resource, "resource is null");
    this.tableId = tableId;

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

  public ProblemReport(TableId tableId, ProblemType problemType, String resource, String server,
      Throwable e) {
    this(tableId, problemType, resource, server, e, System.currentTimeMillis());
  }

  public ProblemReport(TableId tableId, ProblemType problemType, String resource, Throwable e) {
    this(tableId, problemType, resource, null, e);
  }

  private ProblemReport(TableId table, ProblemType problemType, String resource, byte[] enc)
      throws IOException {
    requireNonNull(table, "table is null");
    requireNonNull(problemType, "problemType is null");
    requireNonNull(resource, "resource is null");
    this.tableId = table;
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

  private void decode(byte[] enc) throws IOException {
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

  void removeFromMetadataTable(ServerContext context) throws Exception {
    Mutation m = new Mutation(new Text("~err_" + tableId));
    m.putDelete(new Text(problemType.name()), new Text(resource));
    MetadataTableUtil.getMetadataTable(context).update(m);
  }

  void saveToMetadataTable(ServerContext context) throws Exception {
    Mutation m = new Mutation(new Text("~err_" + tableId));
    m.put(new Text(problemType.name()), new Text(resource), new Value(encode()));
    MetadataTableUtil.getMetadataTable(context).update(m);
  }

  void removeFromZooKeeper(ServerContext context) throws Exception {
    removeFromZooKeeper(context.getZooReaderWriter(), context);
  }

  void removeFromZooKeeper(ZooReaderWriter zoorw, ServerContext context)
      throws IOException, KeeperException, InterruptedException {
    String zpath = getZPath(context.getZooKeeperRoot());
    zoorw.recursiveDelete(zpath, NodeMissingPolicy.SKIP);
  }

  void saveToZooKeeper(ServerContext context)
      throws IOException, KeeperException, InterruptedException {
    context.getZooReaderWriter().putPersistentData(getZPath(context.getZooKeeperRoot()), encode(),
        NodeExistsPolicy.OVERWRITE);
  }

  private String getZPath(String zkRoot) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    dos.writeUTF(getTableId().canonical());
    dos.writeUTF(getProblemType().name());
    dos.writeUTF(getResource());
    dos.close();
    baos.close();

    return zkRoot + Constants.ZPROBLEMS + "/"
        + Encoding.encodeAsBase64FileName(new Text(baos.toByteArray()));
  }

  static ProblemReport decodeZooKeeperEntry(ServerContext context, String node)
      throws IOException, KeeperException, InterruptedException {
    byte[] bytes = Encoding.decodeBase64FileName(node);

    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    DataInputStream dis = new DataInputStream(bais);

    TableId tableId = TableId.of(dis.readUTF());
    String problemType = dis.readUTF();
    String resource = dis.readUTF();

    String zpath = context.getZooKeeperRoot() + Constants.ZPROBLEMS + "/" + node;
    byte[] enc = context.getZooReaderWriter().getData(zpath);

    return new ProblemReport(tableId, ProblemType.valueOf(problemType), resource, enc);

  }

  public static ProblemReport decodeMetadataEntry(Entry<Key,Value> entry) throws IOException {
    TableId tableId = TableId.of(entry.getKey().getRow().toString().substring("~err_".length()));
    String problemType = entry.getKey().getColumnFamily().toString();
    String resource = entry.getKey().getColumnQualifier().toString();

    return new ProblemReport(tableId, ProblemType.valueOf(problemType), resource,
        entry.getValue().get());
  }

  public TableId getTableId() {
    return tableId;
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
    return tableId.hashCode() + problemType.hashCode() + resource.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof ProblemReport) {
      ProblemReport opr = (ProblemReport) o;
      return tableId.equals(opr.tableId) && problemType.equals(opr.problemType)
          && resource.equals(opr.resource);
    }
    return false;
  }
}
