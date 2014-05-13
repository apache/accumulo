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
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.core.replication.thrift.WalEdits;
import org.apache.accumulo.server.data.ServerMutation;
import org.apache.accumulo.tserver.logger.LogEvents;
import org.apache.accumulo.tserver.logger.LogFileKey;
import org.apache.accumulo.tserver.logger.LogFileValue;
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

    key.write(dos);
    value.write(dos);

    key.tablet = null;
    key.event = LogEvents.MUTATION;
    key.filename = "/accumulo/wals/tserver+port/" + UUID.randomUUID();
    value.mutations = Arrays.<Mutation> asList(new ServerMutation(new Text("badrow")));

    key.write(dos);
    value.write(dos);

    key.event = LogEvents.DEFINE_TABLET;
    key.tablet = new KeyExtent(new Text("1"), null, null);
    key.tid = 3;

    key.write(dos);
    value.write(dos);

    key.tablet = null;
    key.event = LogEvents.MUTATION;
    key.filename = "/accumulo/wals/tserver+port/" + UUID.randomUUID();
    value.mutations = Arrays.<Mutation> asList(new ServerMutation(new Text("row")));

    key.write(dos);
    value.write(dos);

    dos.close();

    AccumuloReplicaSystem ars = new AccumuloReplicaSystem();

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    Entry<Long,WalEdits> entry = ars.getEdits(dis, Long.MAX_VALUE, new ReplicationTarget("peer", "1", "1"));
    WalEdits edits = entry.getValue();

    Assert.assertEquals(2, edits.getEditsSize());
  }

}
