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
package org.apache.accumulo.test.randomwalk.image;

import static com.google.common.base.Charsets.UTF_8;

import java.security.MessageDigest;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;
import org.apache.hadoop.io.Text;

public class Verify extends Test {

  String indexTableName;
  String imageTableName;

  @Override
  public void visit(State state, Properties props) throws Exception {

    Random rand = new Random();

    int maxVerify = Integer.parseInt(props.getProperty("maxVerify"));
    int numVerifications = rand.nextInt(maxVerify - 1) + 1;

    indexTableName = state.getString("indexTableName");
    imageTableName = state.getString("imageTableName");

    Connector conn = state.getConnector();

    Scanner indexScanner = conn.createScanner(indexTableName, new Authorizations());
    Scanner imageScanner = conn.createScanner(imageTableName, new Authorizations());

    String uuid = UUID.randomUUID().toString();

    MessageDigest alg = MessageDigest.getInstance("SHA-1");
    alg.update(uuid.getBytes(UTF_8));

    indexScanner.setRange(new Range(new Text(alg.digest()), null));
    indexScanner.setBatchSize(numVerifications);

    Text curRow = null;
    int count = 0;
    for (Entry<Key,Value> entry : indexScanner) {

      curRow = entry.getKey().getRow();
      String rowToVerify = entry.getValue().toString();

      verifyRow(imageScanner, rowToVerify);

      count++;
      if (count == numVerifications) {
        break;
      }
    }

    if (count != numVerifications && curRow != null) {
      Text lastRow = (Text) state.get("lastIndexRow");
      if (lastRow.compareTo(curRow) != 0) {
        log.error("Verified only " + count + " of " + numVerifications + " - curRow " + curRow + " lastKey " + lastRow);
      }
    }

    int verified = ((Integer) state.get("verified")).intValue() + numVerifications;
    log.debug("Verified " + numVerifications + " - Total " + verified);
    state.set("verified", Integer.valueOf(verified));
  }

  public void verifyRow(Scanner scanner, String row) throws Exception {

    scanner.setRange(new Range(new Text(row)));
    scanner.clearColumns();
    scanner.fetchColumnFamily(Write.CONTENT_COLUMN_FAMILY);
    scanner.fetchColumn(Write.META_COLUMN_FAMILY, Write.SHA1_COLUMN_QUALIFIER);

    Iterator<Entry<Key,Value>> scanIter = scanner.iterator();

    if (scanIter.hasNext() == false) {
      log.error("Found row(" + row + ") in " + indexTableName + " but not " + imageTableName);
      return;
    }

    // get image
    Entry<Key,Value> entry = scanIter.next();
    byte[] imageBytes = entry.getValue().get();

    MessageDigest alg = MessageDigest.getInstance("SHA-1");
    alg.update(imageBytes);
    byte[] localHash = alg.digest();

    // get stored hash
    entry = scanIter.next();
    byte[] storedHash = entry.getValue().get();

    if (localHash.length != storedHash.length) {
      throw new Exception("Hash lens do not match for " + row);
    }

    for (int i = 0; i < localHash.length; i++) {
      if (localHash[i] != storedHash[i]) {
        throw new Exception("Hashes do not match for " + row);
      }
    }
  }
}
