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

import org.apache.accumulo.core.metadata.schema.MetadataSchema.DeletesSection;
import org.junit.jupiter.api.Test;

public class DeleteMetadataTest {

  @Test
  public void encodeRowTest() {
    String path = "/dir/testpath";
    assertEquals(path, DeletesSection.decodeRow(DeletesSection.encodeRow(path)));
    path = "hdfs://localhost:8020/dir/r+/1_table/f$%#";
    assertEquals(path, DeletesSection.decodeRow(DeletesSection.encodeRow(path)));

  }
}
