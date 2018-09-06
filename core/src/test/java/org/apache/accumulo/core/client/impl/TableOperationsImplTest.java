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

import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.easymock.EasyMock;
import org.junit.Test;

public class TableOperationsImplTest {

  @Test
  public void waitForStoreTransitionScannerConfiguredCorrectly() throws Exception {
    final String tableName = "metadata";
    ClientContext context = EasyMock.createMock(ClientContext.class);

    TableOperationsImpl topsImpl = new TableOperationsImpl(context);

    Connector connector = EasyMock.createMock(Connector.class);
    Scanner scanner = EasyMock.createMock(Scanner.class);

    Range range = new KeyExtent(Table.ID.of("1"), null, null).toMetadataRange();

    EasyMock.expect(context.getConnector()).andReturn(connector);
    EasyMock.expect(connector.createScanner(tableName, Authorizations.EMPTY)).andReturn(scanner);

    // Fetch the columns on the scanner
    scanner.fetchColumnFamily(MetadataSchema.TabletsSection.FutureLocationColumnFamily.NAME);
    EasyMock.expectLastCall();
    scanner.fetchColumnFamily(MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME);
    EasyMock.expectLastCall();
    scanner.fetchColumn(
        MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.getColumnFamily(),
        MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.getColumnQualifier());
    EasyMock.expectLastCall();

    // Set the Range
    scanner.setRange(range);
    EasyMock.expectLastCall();

    // IsolatedScanner -- make the verification pass, not really relevant
    EasyMock.expect(scanner.getRange()).andReturn(range).anyTimes();
    EasyMock.expect(scanner.getTimeout(TimeUnit.MILLISECONDS)).andReturn(Long.MAX_VALUE);
    EasyMock.expect(scanner.getBatchTimeout(TimeUnit.MILLISECONDS)).andReturn(Long.MAX_VALUE);
    EasyMock.expect(scanner.getBatchSize()).andReturn(1000);
    EasyMock.expect(scanner.getReadaheadThreshold()).andReturn(100L);

    EasyMock.replay(context, connector, scanner);

    topsImpl.createMetadataScanner(tableName, range);

    EasyMock.verify(context, connector, scanner);
  }

}
