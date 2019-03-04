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
package org.apache.accumulo.core.cli;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.ClientProperty;
import org.junit.Test;

public class TestClientOpts {

  @Test
  public void testBasic() {
    ClientOpts opts = new ClientOpts();
    String[] args = new String[] {"-u", "userabc", "-o", "instance.name=myinst", "-o",
        "instance.zookeepers=zoo1,zoo2", "-o", "auth.type=password", "-o", "auth.principal=user123",
        "-o", "auth.token=mypass"};
    opts.parseArgs("test", args);
    Properties props = opts.getClientProps();
    assertEquals("user123", ClientProperty.AUTH_PRINCIPAL.getValue(props));
    assertTrue(opts.getToken() instanceof PasswordToken);
    assertEquals("myinst", props.getProperty("instance.name"));

    opts = new ClientOpts();
    args = new String[] {"-o", "instance.name=myinst", "-o", "instance.zookeepers=zoo1,zoo2", "-o",
        "auth.type=password", "-o", "auth.token=mypass", "-u", "userabc"};
    opts.parseArgs("test", args);
    props = opts.getClientProps();
    assertEquals("userabc", ClientProperty.AUTH_PRINCIPAL.getValue(props));
    assertTrue(opts.getToken() instanceof PasswordToken);
    assertEquals("myinst", props.getProperty("instance.name"));
  }
}
