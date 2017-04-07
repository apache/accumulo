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
package org.apache.accumulo.core.client.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.SortedSet;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.iterators.DebugIterator;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.hadoop.io.Text;
import org.junit.Test;

/**
 * Test that scanner options are set/unset correctly
 */
public class ScannerOptionsTest {

  /**
   * Test that you properly add and remove iterators from a scanner
   */
  @Test
  public void testAddRemoveIterator() throws Throwable {
    try (ScannerOptions options = new ScannerOptions()) {
      options.addScanIterator(new IteratorSetting(1, "NAME", WholeRowIterator.class));
      assertEquals(1, options.serverSideIteratorList.size());
      options.removeScanIterator("NAME");
      assertEquals(0, options.serverSideIteratorList.size());
    }
  }

  @Test
  public void testIteratorConflict() {
    try (ScannerOptions options = new ScannerOptions()) {
      options.addScanIterator(new IteratorSetting(1, "NAME", DebugIterator.class));
      try {
        options.addScanIterator(new IteratorSetting(2, "NAME", DebugIterator.class));
        fail();
      } catch (IllegalArgumentException e) {}
      try {
        options.addScanIterator(new IteratorSetting(1, "NAME2", DebugIterator.class));
        fail();
      } catch (IllegalArgumentException e) {}
    }
  }

  @Test
  public void testFetchColumn() {
    try (ScannerOptions options = new ScannerOptions()) {
      assertEquals(0, options.getFetchedColumns().size());
      IteratorSetting.Column col = new IteratorSetting.Column(new Text("family"), new Text("qualifier"));
      options.fetchColumn(col);
      SortedSet<Column> fetchedColumns = options.getFetchedColumns();
      assertEquals(1, fetchedColumns.size());
      Column fetchCol = fetchedColumns.iterator().next();
      assertEquals(col.getColumnFamily(), new Text(fetchCol.getColumnFamily()));
      assertEquals(col.getColumnQualifier(), new Text(fetchCol.getColumnQualifier()));
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFetchNullColumn() {
    try (ScannerOptions options = new ScannerOptions()) {
      // Require a non-null instance of Column
      options.fetchColumn((IteratorSetting.Column) null);
    }
  }
}
