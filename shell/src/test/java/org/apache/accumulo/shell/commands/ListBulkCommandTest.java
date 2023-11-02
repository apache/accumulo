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
package org.apache.accumulo.shell.commands;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.accumulo.core.manager.thrift.BulkImportState;
import org.apache.accumulo.core.manager.thrift.BulkImportStatus;
import org.apache.accumulo.core.manager.thrift.ManagerMonitorInfo;
import org.junit.jupiter.api.Test;

public class ListBulkCommandTest {

  /**
   * Test the iterator used by BulkCommand prints correctly
   */
  @Test
  public void testBulkImportListIterator() {
    ManagerMonitorInfo mmi = createMock(ManagerMonitorInfo.class);
    List<BulkImportStatus> bulkImports = new ArrayList<>();

    // tserver 1
    var bis1 = new BulkImportStatus();
    bis1.filename = "file1";
    bis1.startTime = System.currentTimeMillis();
    bis1.state = BulkImportState.COPY_FILES;

    // tserver 2
    var bis2 = new BulkImportStatus();
    bis2.filename = "file2";
    bis2.startTime = System.currentTimeMillis();
    bis2.state = BulkImportState.LOADING;

    bulkImports.add(bis1);
    bulkImports.add(bis2);

    expect(mmi.getBulkImports()).andReturn(bulkImports).once();

    replay(mmi);

    var iter = new BulkImportListIterator(mmi);
    List<String> printed = printLines(iter);

    assertTrue(printed.get(0).stripLeading().startsWith("file1"));
    assertTrue(printed.get(0).endsWith("COPY_FILES"));
    assertTrue(printed.get(1).stripLeading().startsWith("file2"));
    assertTrue(printed.get(1).endsWith("LOADING"));

    verify(mmi);
  }

  private List<String> printLines(Iterator<String> lines) {
    List<String> printed = new ArrayList<>();

    while (lines.hasNext()) {
      String nextLine = lines.next();
      if (nextLine == null) {
        continue;
      }
      printed.add(nextLine);
    }
    return printed;
  }
}
