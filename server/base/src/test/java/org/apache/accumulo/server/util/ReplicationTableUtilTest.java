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
package org.apache.accumulo.server.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.accumulo.core.client.impl.Writer;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.replication.StatusUtil;
import org.apache.accumulo.core.security.Credentials;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Assert;
import org.junit.Test;

/**
 * 
 */
public class ReplicationTableUtilTest {

  @Test
  public void properPathInRow() throws Exception {
    Writer writer = EasyMock.createNiceMock(Writer.class);
    writer.update(EasyMock.anyObject(Mutation.class));
    final List<Mutation> mutations = new ArrayList<Mutation>();

    // Mock a Writer to just add the mutation to a list
    EasyMock.expectLastCall().andAnswer(new IAnswer<Object>() {
      public Object answer() {
        mutations.add(((Mutation) EasyMock.getCurrentArguments()[0]));
        return null;
      }
    });

    EasyMock.replay(writer);

    Credentials creds = new Credentials("root", new PasswordToken(""));

    // Magic hook to create a Writer
    ReplicationTableUtil.addWriter(creds, writer);

    // Example file seen coming out of LogEntry
    UUID uuid = UUID.randomUUID();
    String myFile = "file:////home/user/accumulo/wal/server+port/" + uuid;

    ReplicationTableUtil.updateFiles(creds, new KeyExtent(new Text("1"), null, null), Collections.singleton(myFile), StatusUtil.newFile());

    Assert.assertEquals(1, mutations.size());
    Mutation m = mutations.get(0);

    Assert.assertEquals("file:/home/user/accumulo/wal/server+port/" + uuid, new Text(m.getRow()).toString());

    List<ColumnUpdate> updates = m.getUpdates();
    Assert.assertEquals(1, updates.size());
    ColumnUpdate update = updates.get(0);

    Assert.assertEquals(StatusSection.NAME.toString(), new Text(update.getColumnFamily()).toString());
    Assert.assertEquals("1", new Text(update.getColumnQualifier()).toString());
    Assert.assertEquals(StatusUtil.newFileValue(), new Value(update.getValue()));
  }

}
