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

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.iteratorsImpl.system.DeletingIterator.Behavior;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.junit.jupiter.api.Test;

public class DeleteFailIT extends AccumuloClusterHarness {

  @Test
  public void testFail() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      NewTableConfiguration ntc = new NewTableConfiguration();
      ntc.setProperties(
          Collections.singletonMap(Property.TABLE_DELETE_BEHAVIOR.getKey(), Behavior.FAIL.name()));
      c.tableOperations().create(tableName, ntc);

      try (BatchWriter writer = c.createBatchWriter(tableName)) {
        Mutation m = new Mutation("1234");
        m.putDelete("f1", "q1", 2L);
        writer.addMutation(m);
      }

      try (Scanner scanner = c.createScanner(tableName, Authorizations.EMPTY)) {
        assertThrows(RuntimeException.class, () -> scanner.forEach(e -> {}),
            "Expected scan to fail because  deletes are present.");
      }
    }
  }
}
