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
package org.apache.accumulo.test;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.server.cli.ClientOpts;
import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.hadoop.io.Text;

public class GCLotsOfCandidatesTest {
  public static void main(String args[]) throws AccumuloException, AccumuloSecurityException, TableNotFoundException, MutationsRejectedException {
    ClientOpts opts = new ClientOpts();
    BatchWriterOpts bwOpts = new BatchWriterOpts();
    opts.parseArgs(GCLotsOfCandidatesTest.class.getName(), args, bwOpts);
    
    Connector conn = opts.getConnector();
    conn.securityOperations().grantTablePermission(conn.whoami(), Constants.METADATA_TABLE_NAME, TablePermission.WRITE);
    BatchWriter bw = conn.createBatchWriter(Constants.METADATA_TABLE_NAME, bwOpts.getBatchWriterConfig());
    
    for (int i = 0; i < 100000; ++i) {
      final Text emptyText = new Text("");
      Text row = new Text(String.format("%s%s%020d%s", Constants.METADATA_DELETE_FLAG_PREFIX, "/", i,
          "aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffffgggggggggghhhhhhhhhhiiiiiiiiiijjjjjjjjjj"));
      Mutation delFlag = new Mutation(row);
      delFlag.put(emptyText, emptyText, new Value(new byte[] {}));
      bw.addMutation(delFlag);
    }
    bw.close();
  }
}
