/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.conf;

import static org.junit.Assert.assertThrows;

import org.apache.accumulo.core.spi.compaction.CheckCompactionConfig;
import org.apache.accumulo.test.categories.SunnyDayTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SunnyDayTests.class)
public class CheckCompactionConfigTest {

  @Test
  public void testThrowsErrorForExternalWithNumThreads() {
    String inputString =
        "[{'name':'small','type': 'internal','maxSize': '16M','numThreads': 8},{'name': 'medium','type': 'internal','maxSize': '128M','numThreads': 4},{'name': 'large','type': 'external','numThreads': 2}]"
            .replaceAll("'", "\"");
    String expectedErrorMsg = "'numThreads' should not be specified for external compactions";

    assertThrows(expectedErrorMsg, IllegalArgumentException.class,
        () -> CheckCompactionConfig.main(new String[] {inputString}));
  }
}
