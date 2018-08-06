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
package org.apache.accumulo.core.client.admin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.conf.Property;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NewTableConfigurationTest {

  private static final Logger log = LoggerFactory.getLogger(NewTableConfigurationTest.class);

  private SortedSet<Text> splits;

  @Before
  public void populateSplits() {
    splits = new TreeSet<Text>();
    splits.add(new Text("ccccc"));
    splits.add(new Text("aaaaa"));
    splits.add(new Text("ddddd"));
    splits.add(new Text("abcde"));
    splits.add(new Text("bbbbb"));
  }

  /**
   * Verify the withSplits/getSplits methods do as expected.
   *
   * The withSplits() takes a SortedSet as its input. Verify that the set orders the data even if
   * input non-ordered.
   *
   * The getSplits should return a SortedSet. Test verifies set performs ordering and the input set
   * and output set are equal.
   */
  @Test
  public void testWithAndGetSplits() {
    NewTableConfiguration ntc = new NewTableConfiguration().withSplits(splits);
    Collection<Text> ntcSplits = ntc.getSplits();
    Iterator<Text> splitIt = splits.iterator();
    Iterator<Text> ntcIt = ntcSplits.iterator();
    while (splitIt.hasNext() && ntcIt.hasNext()) {
      assertEquals(splitIt.next(), ntcIt.next());
    }
    // verify splits is in sorted order
    Iterator<Text> it = splits.iterator();
    Text current = new Text("");
    while (it.hasNext()) {
      Text nxt = it.next();
      assertTrue(current.toString().compareTo(nxt.toString()) < 0);
      current = nxt;
    }
    // verify ntcSplits is in sorted order
    Iterator<Text> it2 = ntcSplits.iterator();
    current = new Text("");
    while (it2.hasNext()) {
      Text nxt = it2.next();
      assertTrue(current.toString().compareTo(nxt.toString()) < 0);
      current = nxt;
    }
  }

  /**
   * Verify that createOffline option
   */
  @Test
  public void testCreateOffline() {
    NewTableConfiguration ntcOffline = new NewTableConfiguration().createOffline();
    assertTrue(ntcOffline.getTableCreationMode() == TableCreationMode.OFFLINE);
    NewTableConfiguration ntcOnline = new NewTableConfiguration();
    assertTrue(ntcOnline.getTableCreationMode() == TableCreationMode.ONLINE);
  }
}
