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

import static org.apache.accumulo.core.metadata.ValidationUtil.validateFileName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Set;

import org.junit.jupiter.api.Test;

public class ValidationUtilTest {

  @Test
  public void testValidateFileNameSuccess() {
    Set.of("F0001acd.rf", "F0001acd.rf_tmp").forEach(f -> validateFileName(f));
  }

  @Test
  public void testValidateFileNameException() {
    Set.of("", "hdfs://nn:8020/accumulo/tables/2/default_tablet/F0001acd.rf", "./F0001acd.rf",
        "/F0001acd.rf").forEach(f -> {
          var e = assertThrows(IllegalArgumentException.class, () -> validateFileName(f));
          assertEquals("Provided filename (" + f + ") is empty or contains invalid characters",
              e.getMessage());
        });
  }

  @Test
  public void testValidateFileNameNull() {
    assertThrows(NullPointerException.class, () -> validateFileName(null));
  }
}
