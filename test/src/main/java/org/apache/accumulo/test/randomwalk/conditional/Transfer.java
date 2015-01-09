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
package org.apache.accumulo.test.randomwalk.conditional;

import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriter.Status;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;
import org.apache.commons.math.distribution.ZipfDistributionImpl;
import org.apache.hadoop.io.Text;

/**
 *
 */
public class Transfer extends Test {

  private static class Account {
    int seq;
    int bal;

    void setBal(String s) {
      bal = Integer.parseInt(s);
    }

    void setSeq(String s) {
      seq = Integer.parseInt(s);
    }

    public String toString() {
      return seq + " " + bal;
    }
  }

  @Override
  public void visit(State state, Properties props) throws Exception {
    String table = state.getString("tableName");
    Random rand = (Random) state.get("rand");
    Connector conn = state.getConnector();

    int numAccts = (Integer) state.get("numAccts");
    // note: non integer exponents are slow

    ZipfDistributionImpl zdiBanks = new ZipfDistributionImpl((Integer) state.get("numBanks"), 1);
    String bank = Utils.getBank(zdiBanks.inverseCumulativeProbability(rand.nextDouble()));
    ZipfDistributionImpl zdiAccts = new ZipfDistributionImpl(numAccts, 1);
    String acct1 = Utils.getAccount(zdiAccts.inverseCumulativeProbability(rand.nextDouble()));
    String acct2 = Utils.getAccount(zdiAccts.inverseCumulativeProbability(rand.nextDouble()));
    while (acct2.equals(acct1)) {
      // intentionally not using zipf distribution to pick on retry
      acct2 = Utils.getAccount(rand.nextInt(numAccts));
    }

    // TODO document how data should be read when using ConditionalWriter
    Scanner scanner = new IsolatedScanner(conn.createScanner(table, Authorizations.EMPTY));

    scanner.setRange(new Range(bank));
    scanner.fetchColumnFamily(new Text(acct1));
    scanner.fetchColumnFamily(new Text(acct2));

    Account a1 = new Account();
    Account a2 = new Account();
    Account a;

    for (Entry<Key,Value> entry : scanner) {
      String cf = entry.getKey().getColumnFamilyData().toString();
      String cq = entry.getKey().getColumnQualifierData().toString();

      if (cf.equals(acct1))
        a = a1;
      else if (cf.equals(acct2))
        a = a2;
      else
        throw new Exception("Unexpected column fam: " + cf);

      if (cq.equals("bal"))
        a.setBal(entry.getValue().toString());
      else if (cq.equals("seq"))
        a.setSeq(entry.getValue().toString());
      else
        throw new Exception("Unexpected column qual: " + cq);
    }

    int amt = rand.nextInt(50);

    log.debug("transfer req " + bank + " " + amt + " " + acct1 + " " + a1 + " " + acct2 + " " + a2);

    if (a1.bal >= amt) {
      ConditionalMutation cm = new ConditionalMutation(bank, new Condition(acct1, "seq").setValue(Utils.getSeq(a1.seq)),
          new Condition(acct2, "seq").setValue(Utils.getSeq(a2.seq)));
      cm.put(acct1, "bal", (a1.bal - amt) + "");
      cm.put(acct2, "bal", (a2.bal + amt) + "");
      cm.put(acct1, "seq", Utils.getSeq(a1.seq + 1));
      cm.put(acct2, "seq", Utils.getSeq(a2.seq + 1));

      ConditionalWriter cw = (ConditionalWriter) state.get("cw");
      Status status = cw.write(cm).getStatus();
      while (status == Status.UNKNOWN) {
        log.debug("retrying transfer " + status);
        status = cw.write(cm).getStatus();
      }
      log.debug("transfer result " + bank + " " + status + " " + a1 + " " + a2);
    }

  }

}
