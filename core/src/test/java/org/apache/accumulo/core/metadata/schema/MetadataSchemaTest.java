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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.MergedColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class MetadataSchemaTest {

  @Test
  public void testDecodeEncodePrevEndRow() {
    assertNull(TabletColumnFamily.decodePrevEndRow(TabletColumnFamily.encodePrevEndRow(null)));

    Text x = new Text();
    assertEquals(x, TabletColumnFamily.decodePrevEndRow(TabletColumnFamily.encodePrevEndRow(x)));

    Text ab = new Text("ab");
    assertEquals(ab, TabletColumnFamily.decodePrevEndRow(TabletColumnFamily.encodePrevEndRow(ab)));
  }

  @Test
  public void testMergedEncodeDecode() {
    assertEquals(new Value(new byte[] {1}), MergedColumnFamily.encodeHasPrevRowColumn(true));
    assertEquals(new Value(new byte[] {0}), MergedColumnFamily.encodeHasPrevRowColumn(false));

    assertTrue(MergedColumnFamily.decodeHasPrevRowColumn(new Value(new byte[] {1})));
    assertFalse(MergedColumnFamily.decodeHasPrevRowColumn(new Value(new byte[] {0})));
    assertFalse(MergedColumnFamily.decodeHasPrevRowColumn(null));
  }
}
