/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test.functional;

import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Iterators;

public class BadIteratorMincIT extends AccumuloClusterHarness {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(1);
  }

  @Test
  public void test() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {

      String tableName = getUniqueNames(1)[0];
      c.tableOperations().create(tableName);
      IteratorSetting is = new IteratorSetting(30, BadIterator.class);
      c.tableOperations().attachIterator(tableName, is, EnumSet.of(IteratorScope.minc));
      try (BatchWriter bw = c.createBatchWriter(tableName)) {
        Mutation m = new Mutation(new Text("r1"));
        m.put("acf", tableName, "1");
        bw.addMutation(m);
      }

      c.tableOperations().flush(tableName, null, null, false);
      sleepUninterruptibly(1, TimeUnit.SECONDS);

      // minc should fail, so there should be no files
      FunctionalTestUtils.checkRFiles(c, tableName, 1, 1, 0, 0);

      // try to scan table
      try (Scanner scanner = c.createScanner(tableName, Authorizations.EMPTY)) {
        int count = Iterators.size(scanner.iterator());
        assertEquals(1, count, "Did not see expected # entries " + count);

        // remove the bad iterator
        c.tableOperations().removeIterator(tableName, BadIterator.class.getSimpleName(),
            EnumSet.of(IteratorScope.minc));

        sleepUninterruptibly(5, TimeUnit.SECONDS);

        // minc should complete
        FunctionalTestUtils.checkRFiles(c, tableName, 1, 1, 1, 1);

        count = Iterators.size(scanner.iterator());

        if (count != 1) {
          throw new Exception("Did not see expected # entries " + count);
        }

        // now try putting bad iterator back and deleting the table
        c.tableOperations().attachIterator(tableName, is, EnumSet.of(IteratorScope.minc));
        try (BatchWriter bw = c.createBatchWriter(tableName)) {
          Mutation m = new Mutation(new Text("r2"));
          m.put("acf", tableName, "1");
          bw.addMutation(m);
        }

        // make sure property is given time to propagate
        sleepUninterruptibly(500, TimeUnit.MILLISECONDS);

        c.tableOperations().flush(tableName, null, null, false);

        // make sure the flush has time to start
        sleepUninterruptibly(1, TimeUnit.SECONDS);

        // this should not hang
        c.tableOperations().delete(tableName);
      }
    }
  }
}
