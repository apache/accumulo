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
package org.apache.accumulo.core.metadata.schema;

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.DIR;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.ECOMP;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FILES;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.SCANS;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.SELECTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ExternalCompactionColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ScanFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletMetadataCheck.ResolvedColumns;
import org.junit.jupiter.api.Test;

public class TabletMetadataCheckTest {

  @Test
  public void testResolvedColumns() {
    // check columns is empty set when all columns is used
    var resolved1 = new ResolvedColumns(TabletMetadataCheck.ALL_COLUMNS);
    assertTrue(resolved1.getFamilies().isEmpty());
    assertTrue(TabletMetadataCheck.ALL_RESOLVED_COLUMNS.getFamilies().isEmpty());

    // Add some column types and verify resolved families is not empty and is correct
    var expectedColumnTypes = Set.of(PREV_ROW, SELECTED, FILES, ECOMP, SCANS, DIR);
    var expectedFamilies = Set
        .of(ServerColumnFamily.NAME, TabletColumnFamily.NAME, DataFileColumnFamily.NAME,
            ScanFileColumnFamily.NAME, ExternalCompactionColumnFamily.NAME)
        .stream().map(family -> new ArrayByteSequence(family.copyBytes()))
        .collect(Collectors.toSet());
    var resolved2 = new ResolvedColumns(expectedColumnTypes);
    assertEquals(expectedColumnTypes, resolved2.getColumns());
    assertEquals(ColumnType.resolveFamilies(resolved2.getColumns()), resolved2.getFamilies());
    assertEquals(expectedFamilies, resolved2.getFamilies());
  }

}
