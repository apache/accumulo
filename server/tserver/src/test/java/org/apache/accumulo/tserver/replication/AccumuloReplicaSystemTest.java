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
package org.apache.accumulo.tserver.replication;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.server.data.ServerMutation;
import org.apache.accumulo.tserver.logger.LogEvents;
import org.apache.accumulo.tserver.logger.LogFileKey;
import org.apache.accumulo.tserver.logger.LogFileValue;
import org.apache.accumulo.tserver.replication.AccumuloReplicaSystem.WalReplication;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

/**
 * 
 */
public class AccumuloReplicaSystemTest {

  @Test
  public void onlyChooseMutationsForDesiredTable() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);

    LogFileKey key = new LogFileKey();
    LogFileValue value = new LogFileValue();

    // What is seq used for?
    key.seq = 1l;

    /*
     * Disclaimer: the following series of LogFileKey and LogFileValue pairs have *no* bearing
     * whatsoever in reality regarding what these entries would actually look like in a WAL.
     * They are solely for testing that each LogEvents is handled, order is not important. 
     */
    key.event = LogEvents.DEFINE_TABLET;
    key.tablet = new KeyExtent(new Text("1"), null, null);
    key.tid = 1;

    key.write(dos);
    value.write(dos);

    key.tablet = null;
    key.event = LogEvents.MUTATION;
    key.filename = "/accumulo/wals/tserver+port/" + UUID.randomUUID();
    value.mutations = Arrays.<Mutation> asList(new ServerMutation(new Text("row")));

    key.write(dos);
    value.write(dos);

    key.event = LogEvents.DEFINE_TABLET;
    key.tablet = new KeyExtent(new Text("2"), null, null);
    key.tid = 2;
    value.mutations = Collections.emptyList();

    key.write(dos);
    value.write(dos);

    key.event = LogEvents.OPEN;
    key.tid = LogFileKey.VERSION;
    key.tserverSession = "foobar";

    key.write(dos);
    value.write(dos);

    key.tablet = null;
    key.event = LogEvents.MUTATION;
    key.filename = "/accumulo/wals/tserver+port/" + UUID.randomUUID();
    value.mutations = Arrays.<Mutation> asList(new ServerMutation(new Text("badrow")));

    key.write(dos);
    value.write(dos);

    key.event = LogEvents.COMPACTION_START;
    key.tid = 2;
    key.filename = "/accumulo/tables/1/t-000001/A000001.rf";
    value.mutations = Collections.emptyList();

    key.write(dos);
    value.write(dos);

    key.event = LogEvents.DEFINE_TABLET;
    key.tablet = new KeyExtent(new Text("1"), null, null);
    key.tid = 3;
    value.mutations = Collections.emptyList();

    key.write(dos);
    value.write(dos);

    key.event = LogEvents.COMPACTION_FINISH;
    key.tid = 6;
    value.mutations = Collections.emptyList();

    key.write(dos);
    value.write(dos);

    key.tablet = null;
    key.event = LogEvents.MUTATION;
    key.tid = 3;
    key.filename = "/accumulo/wals/tserver+port/" + UUID.randomUUID();
    value.mutations = Arrays.<Mutation> asList(new ServerMutation(new Text("row")));

    key.write(dos);
    value.write(dos);

    dos.close();

    Map<String,String> confMap = new HashMap<>();
    confMap.put(Property.REPLICATION_NAME.getKey(), "source");
    AccumuloConfiguration conf = new ConfigurationCopy(confMap);

    AccumuloReplicaSystem ars = new AccumuloReplicaSystem();
    ars.setConf(conf);

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    WalReplication repl = ars.getEdits(dis, Long.MAX_VALUE, new ReplicationTarget("peer", "1", "1"));

    Assert.assertEquals(9, repl.entriesConsumed);
    Assert.assertEquals(2, repl.walEdits.getEditsSize());
    Assert.assertEquals(2, repl.sizeInRecords);
    Assert.assertNotEquals(0, repl.sizeInBytes);
  }

  @Test
  public void mutationsNotReReplicatedToPeers() throws Exception {
    AccumuloReplicaSystem ars = new AccumuloReplicaSystem();
    Map<String,String> confMap = new HashMap<>();
    confMap.put(Property.REPLICATION_NAME.getKey(), "source");
    AccumuloConfiguration conf = new ConfigurationCopy(confMap);

    ars.setConf(conf);

    LogFileValue value = new LogFileValue();
    value.mutations = new ArrayList<>();
    
    Mutation m = new Mutation("row");
    m.put("", "", new Value(new byte[0]));
    value.mutations.add(m);

    m = new Mutation("row2");
    m.put("", "", new Value(new byte[0]));
    m.addReplicationSource("peer");
    value.mutations.add(m);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);

    // Replicate our 2 mutations to "peer", from tableid 1 to tableid 1
    ars.writeValueAvoidingReplicationCycles(out, value, new ReplicationTarget("peer", "1", "1"));

    out.close();

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream in = new DataInputStream(bais);

    int numMutations = in.readInt();
    Assert.assertEquals(1, numMutations);

    m = new Mutation();
    m.readFields(in);

    Assert.assertEquals("row", new String(m.getRow()));
    Assert.assertEquals(1, m.getReplicationSources().size());
    Assert.assertTrue("Expected source cluster to be listed in mutation replication source", m.getReplicationSources().contains("source"));
  }

  @Test
  public void endOfFileExceptionOnWalImpliesFullyReplicated() throws Exception {
    Assert.fail("Not yet implemented...");
  }
}
