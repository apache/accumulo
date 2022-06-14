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
package org.apache.accumulo.core.clientImpl;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.partialMockBuilder;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.jupiter.api.Test;

public class ScannerBaseTest {

  @Test
  public void testScannerBaseForEach() throws Exception {
    Map<Key,Value> expected =
        Map.of(new Key("row1", "cf1", "cq1"), new Value("v1"), new Key("row2", "cf1", "cq1"),
            new Value("v2"), new Key("row3", "cf1", "cq1"), new Value("v3"));

    // mock ScannerOptions subclass, because EasyMock can't mock ScannerBase, an interface;
    // only the iterator method is mocked, because the forEach method should only call iterator()
    ScannerBase scanner =
        partialMockBuilder(ScannerOptions.class).addMockedMethod("iterator").createMock();
    expect(scanner.iterator()).andReturn(expected.entrySet().iterator()).once();
    replay(scanner);

    // check the results from forEach; they should match what iterator() returns
    Map<Key,Value> actual = new HashMap<>();
    scanner.forEach((k, v) -> actual.put(k, v));
    assertEquals(expected, actual);

    verify(scanner);
  }
}
