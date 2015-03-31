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
package org.apache.accumulo.server.replication;

import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.ReplicationSection;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 *
 */
public class PrintReplicationRecords implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(PrintReplicationRecords.class);

  private Connector conn;
  private PrintStream out;
  private SimpleDateFormat sdf;

  public PrintReplicationRecords(Connector conn, PrintStream out) {
    this.conn = conn;
    this.out = out;
    this.sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
  }

  @Override
  public void run() {
    Scanner s;

    out.println(sdf.format(new Date()) + " Replication entries from metadata table");
    out.println("------------------------------------------------------------------");
    try {
      s = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    } catch (TableNotFoundException e) {
      log.error("Metadata table does not exist");
      return;
    }

    s.setRange(ReplicationSection.getRange());
    s.fetchColumnFamily(ReplicationSection.COLF);
    for (Entry<Key,Value> entry : s) {
      try {
        out.println(entry.getKey().toStringNoTruncate() + "=" + ProtobufUtil.toString(Status.parseFrom(entry.getValue().get())));
      } catch (InvalidProtocolBufferException e) {
        out.println(entry.getKey().toStringNoTruncate() + "= Could not deserialize Status message");
      }
    }

    out.println();
    out.println(sdf.format(new Date()) + " Replication entries from replication table");
    out.println("--------------------------------------------------------------------");

    try {
      s = conn.createScanner(ReplicationTable.NAME, Authorizations.EMPTY);
    } catch (TableNotFoundException e) {
      log.error("Replication table does not exist");
      return;
    }

    for (Entry<Key,Value> entry : s) {
      try {
        out.println(entry.getKey().toStringNoTruncate() + "=" + ProtobufUtil.toString(Status.parseFrom(entry.getValue().get())));
      } catch (InvalidProtocolBufferException e) {
        out.println(entry.getKey().toStringNoTruncate() + "= Could not deserialize Status message");
      }
    }
  }
}
