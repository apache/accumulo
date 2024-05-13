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
package org.apache.accumulo.test.ample;

import static org.apache.accumulo.test.ample.metadata.TestAmple.testAmpleServerContext;

import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.test.ample.metadata.TestAmple.TestServerAmpleImpl;
import org.easymock.EasyMock;

public class TestAmpleUtil {

  public static Manager mockWithAmple(ServerContext context, TestServerAmpleImpl ample) {
    Manager manager = EasyMock.mock(Manager.class);
    EasyMock.expect(manager.getContext()).andReturn(testAmpleServerContext(context, ample))
        .atLeastOnce();
    EasyMock.replay(manager);
    return manager;
  }

}
