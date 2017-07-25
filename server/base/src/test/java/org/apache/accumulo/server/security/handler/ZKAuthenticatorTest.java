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
package org.apache.accumulo.server.security.handler;

import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.util.ByteArraySet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import junit.framework.TestCase;

public class ZKAuthenticatorTest extends TestCase {
  private static final Logger log = LoggerFactory.getLogger(ZKAuthenticatorTest.class);

  public void testPermissionIdConversions() {
    for (SystemPermission s : SystemPermission.values())
      assertTrue(s.equals(SystemPermission.getPermissionById(s.getId())));

    for (TablePermission s : TablePermission.values())
      assertTrue(s.equals(TablePermission.getPermissionById(s.getId())));
  }

  public void testAuthorizationConversion() {
    ByteArraySet auths = new ByteArraySet();
    for (int i = 0; i < 300; i += 3)
      auths.add(Integer.toString(i).getBytes());

    Authorizations converted = new Authorizations(auths);
    byte[] test = ZKSecurityTool.convertAuthorizations(converted);
    Authorizations test2 = ZKSecurityTool.convertAuthorizations(test);
    assertTrue(auths.size() == test2.size());
    for (byte[] s : auths) {
      assertTrue(test2.contains(s));
    }
  }

  public void testSystemConversion() {
    Set<SystemPermission> perms = new TreeSet<>();
    for (SystemPermission s : SystemPermission.values())
      perms.add(s);

    Set<SystemPermission> converted = ZKSecurityTool.convertSystemPermissions(ZKSecurityTool.convertSystemPermissions(perms));
    assertTrue(perms.size() == converted.size());
    for (SystemPermission s : perms)
      assertTrue(converted.contains(s));
  }

  public void testTableConversion() {
    Set<TablePermission> perms = new TreeSet<>();
    for (TablePermission s : TablePermission.values())
      perms.add(s);

    Set<TablePermission> converted = ZKSecurityTool.convertTablePermissions(ZKSecurityTool.convertTablePermissions(perms));
    assertTrue(perms.size() == converted.size());
    for (TablePermission s : perms)
      assertTrue(converted.contains(s));
  }

  public void testEncryption() {
    byte[] rawPass = "myPassword".getBytes();
    byte[] storedBytes;
    try {
      storedBytes = ZKSecurityTool.createPass(rawPass);
      assertTrue(ZKSecurityTool.checkPass(rawPass, storedBytes));
    } catch (AccumuloException e) {
      log.error("{}", e.getMessage(), e);
      assertTrue(false);
    }
  }
}
