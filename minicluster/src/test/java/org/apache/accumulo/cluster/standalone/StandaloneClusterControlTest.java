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
package org.apache.accumulo.cluster.standalone;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.aryEq;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;

import org.easymock.EasyMock;
import org.junit.Test;

import com.google.common.collect.Maps;

public class StandaloneClusterControlTest {

  @Test
  public void testPaths() {
    String accumuloHome = "/usr/lib/accumulo", accumuloConfDir = "/etc/accumulo/conf", accumuloServerConfDir = "/etc/accumulo/conf/server";

    StandaloneClusterControl control = new StandaloneClusterControl(accumuloHome, accumuloConfDir, accumuloServerConfDir, "", "");

    assertEquals(accumuloHome, control.accumuloHome);
    assertEquals(accumuloConfDir, control.clientAccumuloConfDir);
    assertEquals(accumuloServerConfDir, control.serverAccumuloConfDir);

    assertEquals(accumuloHome + "/bin/accumulo", control.accumuloPath);
    assertEquals(accumuloHome + "/bin/accumulo-service", control.accumuloServicePath);
  }

  @Test
  public void mapreduceLaunchesLocally() throws Exception {
    final String accumuloUtilPath = "/usr/lib/accumulo/bin/accumulo-util";
    final String jar = "/home/user/my_project.jar";
    final Class<?> clz = Object.class;
    final String myClass = clz.getName();
    StandaloneClusterControl control = EasyMock.createMockBuilder(StandaloneClusterControl.class).addMockedMethod("exec", String.class, String[].class)
        .addMockedMethod("getAccumuloUtilPath").addMockedMethod("getJarFromClass", Class.class).createMock();

    final String[] toolArgs = new String[] {"-u", "user", "-p", "password"};
    final String[] expectedCommands = new String[4 + toolArgs.length];

    int i = 0;
    expectedCommands[i++] = accumuloUtilPath;
    expectedCommands[i++] = "hadoop-jar";
    expectedCommands[i++] = jar;
    expectedCommands[i++] = myClass;
    for (int j = 0; j < toolArgs.length; j++) {
      expectedCommands[i + j] = quote(toolArgs[j]);
    }

    expect(control.getAccumuloUtilPath()).andReturn(accumuloUtilPath);
    expect(control.getJarFromClass(anyObject(Class.class))).andReturn(jar);
    expect(control.exec(eq("localhost"), aryEq(expectedCommands))).andReturn(Maps.immutableEntry(0, ""));

    replay(control);

    // Give a fake Class -- we aren't verifying the actual class passed in
    control.execMapreduceWithStdout(clz, toolArgs);

    verify(control);
  }

  private String quote(String word) {
    return "'" + word + "'";
  }
}
