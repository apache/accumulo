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
package org.apache.accumulo.core.metadata;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class StoredTabletFileTest {

  @Test
  public void fileConversionTest() {
    String s21 = "hdfs://localhost:8020/accumulo/tables/1/t-0000000/A000003v.rf";
    String s31 =
        "{\"path\":\"hdfs://localhost:8020/accumulo/tables/1/t-0000000/A000003v.rf\",\"startRow\":\"\",\"endRow\":\"\"}";
    String s31_untrimmed =
        "   {  \"path\":\"hdfs://localhost:8020/accumulo/tables/1/t-0000000/A000003v.rf\",\"startRow\":\"\",\"endRow\":\"\"  }   ";

    assertTrue(StoredTabletFile.fileNeedsConversion(s21));
    assertFalse(StoredTabletFile.fileNeedsConversion(s31));
    assertFalse(StoredTabletFile.fileNeedsConversion(s31_untrimmed));
  }
}
