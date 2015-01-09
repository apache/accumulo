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
package org.apache.accumulo.examples.simple.filedata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.PeekingIterator;

/**
 * Retrieves file data based on the hash of the file. Used by the {@link org.apache.accumulo.examples.simple.dirlist.Viewer}. See README.dirlist for
 * instructions.
 */
public class FileDataQuery {
  private Connector conn = null;
  List<Entry<Key,Value>> lastRefs;
  private ChunkInputStream cis;
  Scanner scanner;

  public FileDataQuery(String instanceName, String zooKeepers, String user, AuthenticationToken token, String tableName, Authorizations auths)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    ZooKeeperInstance instance = new ZooKeeperInstance(ClientConfiguration.loadDefault().withInstance(instanceName).withZkHosts(zooKeepers));
    conn = instance.getConnector(user, token);
    lastRefs = new ArrayList<Entry<Key,Value>>();
    cis = new ChunkInputStream();
    scanner = conn.createScanner(tableName, auths);
  }

  public List<Entry<Key,Value>> getLastRefs() {
    return lastRefs;
  }

  public ChunkInputStream getData(String hash) throws IOException {
    scanner.setRange(new Range(hash));
    scanner.setBatchSize(1);
    lastRefs.clear();
    PeekingIterator<Entry<Key,Value>> pi = new PeekingIterator<Entry<Key,Value>>(scanner.iterator());
    if (pi.hasNext()) {
      while (!pi.peek().getKey().getColumnFamily().equals(FileDataIngest.CHUNK_CF)) {
        lastRefs.add(pi.peek());
        pi.next();
      }
    }
    cis.clear();
    cis.setSource(pi);
    return cis;
  }

  public String getSomeData(String hash, int numBytes) throws IOException {
    ChunkInputStream is = getData(hash);
    byte[] buf = new byte[numBytes];
    if (is.read(buf) >= 0) {
      return new String(buf);
    } else {
      return "";
    }
  }
}
