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
package org.apache.accumulo.core.client.security;

import java.util.HashSet;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class SecurityErrorCodeTest {

  @Test
  public void testEnumsSame() {
    HashSet<String> secNames1 = new HashSet<>();
    HashSet<String> secNames2 = new HashSet<>();

    for (SecurityErrorCode sec : SecurityErrorCode.values())
      secNames1.add(sec.name());

    for (org.apache.accumulo.core.client.impl.thrift.SecurityErrorCode sec : org.apache.accumulo.core.client.impl.thrift.SecurityErrorCode.values())
      secNames2.add(sec.name());

    Assert.assertEquals(secNames1, secNames2);
  }
}
