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
package org.apache.accumulo.core.client.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.accumulo.core.clientImpl.ImportConfigurationImpl;
import org.junit.jupiter.api.Test;

public class ImportConfigurationTest {

  @Test
  public void testEmpty() {
    ImportConfiguration ic = ImportConfiguration.empty();
    assertEquals(ImportConfiguration.EMPTY, ic);
    assertFalse(ic.isKeepOffline());
    assertFalse(ic.isKeepMappings());
  }

  @Test
  public void testErrors() {
    ImportConfiguration ic = new ImportConfigurationImpl();
    assertThrows(IllegalStateException.class, ic::isKeepMappings);
    assertThrows(IllegalStateException.class, ic::isKeepOffline);
    ImportConfigurationImpl ic2 = (ImportConfigurationImpl) ImportConfiguration.builder().build();
    assertThrows(IllegalStateException.class, () -> ic2.setKeepMappings(true));
    assertThrows(IllegalStateException.class, () -> ic2.setKeepOffline(true));
  }

  @Test
  public void testOptions() {
    var ic = ImportConfiguration.builder().setKeepMappings(true).setKeepOffline(true).build();
    assertTrue(ic.isKeepMappings());
    assertTrue(ic.isKeepOffline());
  }
}
