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

import java.util.Map.Entry;

import junit.framework.TestCase;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.server.util.TabletIterator.TabletDeletedException;
import org.apache.hadoop.io.Text;

public class TabletIteratorTest extends TestCase {

  class TestTabletIterator extends TabletIterator {

    private Connector conn;

    public TestTabletIterator(Connector conn) throws Exception {
      super(conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY), MetadataSchema.TabletsSection.getRange(), true, true);
      this.conn = conn;
    }

    @Override
    protected void resetScanner() {
      try {
        Scanner ds = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
        Text tablet = new KeyExtent(new Text("0"), new Text("m"), null).getMetadataEntry();
        ds.setRange(new Range(tablet, true, tablet, true));

        Mutation m = new Mutation(tablet);

        BatchWriter bw = conn.createBatchWriter(MetadataTable.NAME, new BatchWriterConfig());
        for (Entry<Key,Value> entry : ds) {
          Key k = entry.getKey();
          m.putDelete(k.getColumnFamily(), k.getColumnQualifier(), k.getTimestamp());
        }

        bw.addMutation(m);

        bw.close();

      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      super.resetScanner();
    }

  }

  // simulate a merge happening while iterating over tablets
  public void testMerge() throws Exception {
    MockInstance mi = new MockInstance();
    Connector conn = mi.getConnector("", new PasswordToken(""));

    KeyExtent ke1 = new KeyExtent(new Text("0"), new Text("m"), null);
    Mutation mut1 = ke1.getPrevRowUpdateMutation();
    TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.put(mut1, new Value("/d1".getBytes()));

    KeyExtent ke2 = new KeyExtent(new Text("0"), null, null);
    Mutation mut2 = ke2.getPrevRowUpdateMutation();
    TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.put(mut2, new Value("/d2".getBytes()));

    BatchWriter bw1 = conn.createBatchWriter(MetadataTable.NAME, new BatchWriterConfig());
    bw1.addMutation(mut1);
    bw1.addMutation(mut2);
    bw1.close();

    TestTabletIterator tabIter = new TestTabletIterator(conn);

    try {
      while (tabIter.hasNext()) {
        tabIter.next();
      }
      assertTrue(false);
    } catch (TabletDeletedException tde) {}
  }
}
