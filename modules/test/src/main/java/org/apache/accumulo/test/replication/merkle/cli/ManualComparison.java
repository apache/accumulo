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
package org.apache.accumulo.test.replication.merkle.cli;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;

import com.beust.jcommander.Parameter;

/**
 * Accepts two table names and enumerates all key-values pairs in both checking for correctness. All differences between the two tables will be printed to the
 * console.
 */
public class ManualComparison {

  public static class ManualComparisonOpts extends ClientOpts {
    @Parameter(names = {"--table1"}, required = true, description = "First table")
    public String table1;

    @Parameter(names = {"--table2"}, required = true, description = "First table")
    public String table2;
  }

  public static void main(String[] args) throws Exception {
    ManualComparisonOpts opts = new ManualComparisonOpts();
    opts.parseArgs("ManualComparison", args);

    Connector conn = opts.getConnector();

    Scanner s1 = conn.createScanner(opts.table1, Authorizations.EMPTY), s2 = conn.createScanner(opts.table2, Authorizations.EMPTY);
    Iterator<Entry<Key,Value>> iter1 = s1.iterator(), iter2 = s2.iterator();
    boolean incrementFirst = true, incrementSecond = true;

    Entry<Key,Value> entry1 = iter1.next(), entry2 = iter2.next();
    while (iter1.hasNext() && iter2.hasNext()) {
      if (incrementFirst) {
        entry1 = iter1.next();
      }
      if (incrementSecond) {
        entry2 = iter2.next();
      }
      incrementFirst = false;
      incrementSecond = false;

      if (!entry1.equals(entry2)) {

        if (entry1.getKey().compareTo(entry2.getKey()) < 0) {
          System.out.println("Exist in original " + entry1);
          incrementFirst = true;
        } else if (entry2.getKey().compareTo(entry1.getKey()) < 0) {
          System.out.println("Exist in replica " + entry2);
          incrementSecond = true;
        } else {
          System.out.println("Differ... " + entry1 + " " + entry2);
          incrementFirst = true;
          incrementSecond = true;
        }
      } else {
        incrementFirst = true;
        incrementSecond = true;
      }
    }

    System.out.println("\nExtra entries from " + opts.table1);
    while (iter1.hasNext()) {
      System.out.println(iter1.next());
    }

    System.out.println("\nExtra entries from " + opts.table2);
    while (iter2.hasNext()) {
      System.out.println(iter2.next());
    }
  }
}
