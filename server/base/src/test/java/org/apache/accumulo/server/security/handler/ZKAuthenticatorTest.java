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
package org.apache.accumulo.server.security.handler;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.matches;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.util.ByteArraySet;
import org.apache.accumulo.server.MockServerContext;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.Test;

public class ZKAuthenticatorTest {

  @Test
  public void testPermissionIdConversions() {
    for (SystemPermission s : SystemPermission.values()) {
      assertEquals(s, SystemPermission.getPermissionById(s.getId()));
    }

    for (TablePermission s : TablePermission.values()) {
      assertEquals(s, TablePermission.getPermissionById(s.getId()));
    }
  }

  @Test
  public void testAuthorizationConversion() {
    ByteArraySet auths = new ByteArraySet();
    for (int i = 0; i < 300; i += 3) {
      auths.add(Integer.toString(i).getBytes());
    }

    Authorizations converted = new Authorizations(auths);
    byte[] test = ZKSecurityTool.convertAuthorizations(converted);
    Authorizations test2 = ZKSecurityTool.convertAuthorizations(test);
    assertEquals(auths.size(), test2.size());
    for (byte[] s : auths) {
      assertTrue(test2.contains(s));
    }
  }

  @Test
  public void testSystemConversion() {
    Set<SystemPermission> perms = new TreeSet<>();
    Collections.addAll(perms, SystemPermission.values());

    Set<SystemPermission> converted =
        ZKSecurityTool.convertSystemPermissions(ZKSecurityTool.convertSystemPermissions(perms));
    assertEquals(perms.size(), converted.size());
    for (SystemPermission s : perms) {
      assertTrue(converted.contains(s));
    }
  }

  @Test
  public void testTableConversion() {
    Set<TablePermission> perms = new TreeSet<>();
    Collections.addAll(perms, TablePermission.values());

    Set<TablePermission> converted =
        ZKSecurityTool.convertTablePermissions(ZKSecurityTool.convertTablePermissions(perms));
    assertEquals(perms.size(), converted.size());
    for (TablePermission s : perms) {
      assertTrue(converted.contains(s));
    }
  }

  @Test
  public void testEncryption() throws AccumuloException {
    byte[] rawPass = "myPassword".getBytes(UTF_8);
    byte[] storedBytes;

    storedBytes = ZKSecurityTool.createPass(rawPass.clone());
    assertTrue(ZKSecurityTool.checkCryptPass(rawPass.clone(), storedBytes));
  }

  @Deprecated
  @Test
  public void testOutdatedEncryption() throws AccumuloException {
    byte[] rawPass = "myPassword".getBytes();
    byte[] storedBytes;

    storedBytes = ZKSecurityTool.createOutdatedPass(rawPass);
    assertTrue(ZKSecurityTool.checkPass(rawPass, storedBytes));
  }

  @Test
  public void testEncryptionDifference() throws AccumuloException {
    byte[] rawPass = "myPassword".getBytes();
    @SuppressWarnings("deprecation")
    byte[] storedBytes = ZKSecurityTool.createOutdatedPass(rawPass);
    assertFalse(ZKSecurityTool.checkCryptPass(rawPass, storedBytes));
  }

  @Test
  public void testUserAuthentication() throws Exception {
    // testing the usecase when trying to authenticate with the new hash type
    String principal = "myTestUser";
    byte[] rawPass = "myPassword".getBytes(UTF_8);
    // creating hash with up to date algorithm
    byte[] newHash = ZKSecurityTool.createPass(rawPass.clone());

    // mocking zk interaction
    ServerContext context = MockServerContext.getWithZK(InstanceId.of("example"), "", 30_000);
    ZooReaderWriter zr = createMock(ZooReaderWriter.class);
    expect(context.getZooReader()).andReturn(zr).anyTimes();
    ZooKeeper zk = createMock(ZooKeeper.class);
    expect(zk.getChildren(anyObject(), anyObject())).andReturn(Arrays.asList(principal)).anyTimes();
    expect(zk.exists(matches("/accumulo/example/users/" + principal), anyObject(Watcher.class)))
        .andReturn(new Stat()).anyTimes();
    expect(zr.getZooKeeper()).andReturn(zk).anyTimes();
    expect(zk.getData(matches("/accumulo/example/users/" + principal), anyObject(), anyObject()))
        .andReturn(newHash).once();
    replay(context, zr, zk);

    // creating authenticator
    ZKAuthenticator auth = new ZKAuthenticator();
    auth.initialize(context);

    PasswordToken token = new PasswordToken(rawPass.clone());
    // verifying that if the new type of hash is stored in zk authentication works as expected
    assertTrue(auth.authenticateUser(principal, token));
    verify(context, zr, zk);
  }

  @Test
  public void testUserAuthenticationUpdate() throws Exception {
    // testing the usecase when trying to authenticate with the outdated hash type
    String principal = "myTestUser";
    byte[] rawPass = "myPassword".getBytes(UTF_8);
    // creating hash with outdated algorithm
    @SuppressWarnings("deprecation")
    byte[] outdatedHash = ZKSecurityTool.createOutdatedPass(rawPass);

    // mocking zk interaction
    ServerContext context = MockServerContext.getWithZK(InstanceId.of("example"), "", 30_000);
    ZooReaderWriter zr = createMock(ZooReaderWriter.class);
    expect(context.getZooReader()).andReturn(zr).anyTimes();
    expect(context.getZooReaderWriter()).andReturn(zr).anyTimes();
    ZooKeeper zk = createMock(ZooKeeper.class);
    expect(zk.getChildren(anyObject(), anyObject())).andReturn(Arrays.asList(principal)).anyTimes();
    expect(zk.exists(matches("/accumulo/example/users/" + principal), anyObject(Watcher.class)))
        .andReturn(new Stat()).anyTimes();
    expect(zr.getZooKeeper()).andReturn(zk).anyTimes();
    expect(zk.getData(matches("/accumulo/example/users/" + principal), anyObject(), anyObject()))
        .andReturn(outdatedHash).once();
    // expecting that the new hash is pushed to zk
    expect(zr.putPrivatePersistentData(matches("/accumulo/example/users/" + principal), anyObject(),
        anyObject())).andReturn(true).once();
    replay(context, zr, zk);

    // creating authenticator
    ZKAuthenticator auth = new ZKAuthenticator();
    auth.initialize(context);

    PasswordToken token = new PasswordToken(rawPass.clone());
    // verifying that if the outdated type of hash is stored in zk authentication works as expected
    assertTrue(auth.authenticateUser(principal, token));
    verify(context, zr, zk);
  }
}
