/**
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
package org.apache.accumulo.core.client.mock;

import java.util.EnumSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.junit.Assert;
import org.junit.Test;

public class MockTableOperationsTest {
  @Test
  public void testTableNotFound() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
    Instance instance = new MockInstance("topstest");
    Connector conn = instance.getConnector("user", "pass");
    String t = "tableName";
    try {
      conn.tableOperations().attachIterator(t, null);
      Assert.fail();
    } catch (TableNotFoundException e) {}
    try {
      conn.tableOperations().checkIteratorConflicts(t, null, EnumSet.allOf(IteratorScope.class));
      Assert.fail();
    } catch (TableNotFoundException e) {}
    try {
      conn.tableOperations().delete(t);
      Assert.fail();
    } catch (TableNotFoundException e) {}
    try {
      conn.tableOperations().getIteratorSetting(t, null, null);
      Assert.fail();
    } catch (TableNotFoundException e) {}
    try {
      conn.tableOperations().getProperties(t);
      Assert.fail();
    } catch (TableNotFoundException e) {}
    try {
      conn.tableOperations().getSplits(t);
      Assert.fail();
    } catch (TableNotFoundException e) {}
    try {
      conn.tableOperations().listIterators(t);
      Assert.fail();
    } catch (TableNotFoundException e) {}
    try {
      conn.tableOperations().removeIterator(t, null, null);
      Assert.fail();
    } catch (TableNotFoundException e) {}
    try {
      conn.tableOperations().rename(t, t);
      Assert.fail();
    } catch (TableNotFoundException e) {}
    conn.tableOperations().create(t);
    try {
      conn.tableOperations().create(t);
      Assert.fail();
    } catch (TableExistsException e) {}
    try {
      conn.tableOperations().rename(t, t);
      Assert.fail();
    } catch (TableExistsException e) {}
  }
}
