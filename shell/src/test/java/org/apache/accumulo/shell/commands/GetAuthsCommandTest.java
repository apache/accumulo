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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;

import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.Test;

public class GetAuthsCommandTest {

  @Test
  public void removeAccumuloNamespaceTables() {
    Authorizations auths = new Authorizations("AAA", "aaa", "bbb", "BBB");
    GetAuthsCommand cmd = new GetAuthsCommand();
    List<String> sorted = cmd.sortAuthorizations(auths);

    assertNotNull(sorted);
    assertEquals(sorted.size(), 4);

    assertEquals(sorted.get(0), "AAA");
    assertEquals(sorted.get(1), "aaa");
    assertEquals(sorted.get(2), "BBB");
    assertEquals(sorted.get(3), "bbb");
  }
}
