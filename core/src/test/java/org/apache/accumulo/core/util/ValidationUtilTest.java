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
package org.apache.accumulo.core.util;

import static org.apache.accumulo.core.metadata.ValidationUtil.validateFileName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class ValidationUtilTest {
  @ParameterizedTest
  @ValueSource(strings = {"F0001acd.rf", "F0001acd.rf_tmp"})
  public void testValidateFileNameSuccess(String value) {
    validateFileName(value);
  }

  @ParameterizedTest
  @ValueSource(strings = {"hdfs://nn:8020/accumulo/tables/2/default_tablet/F0001acd.rf",
      "./F0001acd.rf", "/F0001acd.rf"})
  public void testValidateFileNameException(String value) {
    Exception ex = assertThrows(IllegalArgumentException.class, () -> validateFileName(value));
    assertEquals("Provided filename (" + value + ") contains invalid characters.", ex.getMessage());
  }

  @Test
  public void testValidateFileNameNull() {
    assertThrows(NullPointerException.class, () -> validateFileName(null));
  }
}
