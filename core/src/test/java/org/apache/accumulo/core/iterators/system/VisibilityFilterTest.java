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
package org.apache.accumulo.core.iterators.system;

import java.io.IOException;
import java.util.HashSet;
import java.util.TreeMap;

import junit.framework.TestCase;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class VisibilityFilterTest extends TestCase {

  public void testBadVisibility() throws IOException {
    TreeMap<Key,Value> tm = new TreeMap<Key,Value>();

    tm.put(new Key("r1", "cf1", "cq1", "A&"), new Value(new byte[0]));
    VisibilityFilter filter = new VisibilityFilter(new SortedMapIterator(tm), new Authorizations("A"), "".getBytes());

    // suppress logging
    Level prevLevel = Logger.getLogger(VisibilityFilter.class).getLevel();
    Logger.getLogger(VisibilityFilter.class).setLevel(Level.FATAL);

    filter.seek(new Range(), new HashSet<ByteSequence>(), false);
    assertFalse(filter.hasTop());

    Logger.getLogger(VisibilityFilter.class).setLevel(prevLevel);
  }

}
