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
package org.apache.accumulo.server.security.delegation;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthenticationTokenKeyManagerTest {
  private static final Logger log = LoggerFactory.getLogger(AuthenticationTokenKeyManagerTest.class);

  // From org.apache.hadoop.security.token.SecretManager
  private static final String DEFAULT_HMAC_ALGORITHM = "HmacSHA1";
  private static final int KEY_LENGTH = 64;
  private static KeyGenerator keyGen;

  @BeforeClass
  public static void setupKeyGenerator() throws Exception {
    // From org.apache.hadoop.security.token.SecretManager
    keyGen = KeyGenerator.getInstance(DEFAULT_HMAC_ALGORITHM);
    keyGen.init(KEY_LENGTH);
  }

  private AuthenticationTokenSecretManager secretManager;
  private ZooAuthenticationKeyDistributor zooDistributor;

  @Before
  public void setupMocks() {
    secretManager = createMock(AuthenticationTokenSecretManager.class);
    zooDistributor = createMock(ZooAuthenticationKeyDistributor.class);
  }

  @Test
  public void testIntervalNotPassed() {
    long updateInterval = 5 * 1000l;
    long tokenLifetime = 100 * 1000l;
    AuthenticationTokenKeyManager keyManager = new AuthenticationTokenKeyManager(secretManager, zooDistributor, updateInterval, tokenLifetime);

    // Have never updated the key
    assertEquals(0l, keyManager.getLastKeyUpdate());

    // Always check for expired keys to remove
    expect(secretManager.removeExpiredKeys(zooDistributor)).andReturn(0);

    replay(secretManager, zooDistributor);

    // Run at time 0. Last run time is still 0. 0 + 5000 > 0, so we won't generate a new key
    keyManager._run(0);

    verify(secretManager, zooDistributor);
  }

  @Test
  public void testIntervalHasPassed() throws Exception {
    long updateInterval = 0 * 1000l;
    long tokenLifetime = 100 * 1000l;
    long runTime = 10l;
    SecretKey secretKey = keyGen.generateKey();

    AuthenticationKey authKey = new AuthenticationKey(1, runTime, runTime + tokenLifetime, secretKey);
    AuthenticationTokenKeyManager keyManager = new AuthenticationTokenKeyManager(secretManager, zooDistributor, updateInterval, tokenLifetime);

    // Have never updated the key
    assertEquals(0l, keyManager.getLastKeyUpdate());

    // Always check for expired keys to remove
    expect(secretManager.removeExpiredKeys(zooDistributor)).andReturn(0);
    expect(secretManager.generateSecret()).andReturn(secretKey);
    secretManager.addKey(authKey);
    expectLastCall().once();
    zooDistributor.advertise(authKey);
    expectLastCall().once();

    replay(secretManager, zooDistributor);

    // Run at time 10. Last run time is still 0. 0 + 10 > 0, so we will generate a new key
    keyManager._run(runTime);

    verify(secretManager, zooDistributor);

    // Last key update time should match when we ran
    assertEquals(runTime, keyManager.getLastKeyUpdate());
    // KeyManager uses the incremented value for the new AuthKey (the current idSeq will match the keyId for the last generated key)
    assertEquals(authKey.getKeyId(), keyManager.getIdSeq());
  }

  private static class MockManager extends AuthenticationTokenKeyManager {

    CountDownLatch latch;

    public MockManager() {
      super(null, null, 0, 0);
    }

    @Override
    public void run() {
      log.info("Thread running");
      latch.countDown();
      super.run();
    }

  }

  @Test(timeout = 30 * 1000)
  public void testStopLoop() throws InterruptedException {
    final MockManager keyManager = EasyMock.createMockBuilder(MockManager.class).addMockedMethod("_run").addMockedMethod("updateStateFromCurrentKeys")
        .createMock();
    keyManager.latch = new CountDownLatch(1);

    // Mock out the _run and updateStateFromCurrentKeys method so we just get the logic from "run()"
    keyManager._run(EasyMock.anyLong());
    expectLastCall().once();
    keyManager.updateStateFromCurrentKeys();
    expectLastCall().once();

    replay(keyManager);

    keyManager.setKeepRunning(true);

    // Wrap another Runnable around our KeyManager so we know when the thread is actually run as it's "async" when the method will actually be run after we call
    // thread.start()
    Thread t = new Thread(keyManager);

    log.info("Starting thread");
    t.start();

    // Wait for the thread to start
    keyManager.latch.await();
    log.info("Latch fired");

    // Wait a little bit to let the first call to _run() happen (avoid exiting the loop before any calls to _run())
    Thread.sleep(1000);

    log.info("Finished waiting, stopping keymanager");

    keyManager.gracefulStop();

    log.info("Waiting for thread to exit naturally");

    t.join();

    verify(keyManager);
  }

  @Test
  public void testExistingKeysAreAddedAtStartup() throws Exception {
    long updateInterval = 0 * 1000l;
    long tokenLifetime = 100 * 1000l;
    SecretKey secretKey1 = keyGen.generateKey(), secretKey2 = keyGen.generateKey();

    AuthenticationKey authKey1 = new AuthenticationKey(1, 0, tokenLifetime, secretKey1), authKey2 = new AuthenticationKey(2, tokenLifetime, tokenLifetime * 2,
        secretKey2);
    AuthenticationTokenKeyManager keyManager = new AuthenticationTokenKeyManager(secretManager, zooDistributor, updateInterval, tokenLifetime);

    // Have never updated the key
    assertEquals(0l, keyManager.getLastKeyUpdate());

    // Always check for expired keys to remove
    expect(zooDistributor.getCurrentKeys()).andReturn(Arrays.asList(authKey1, authKey2));
    secretManager.addKey(authKey1);
    expectLastCall().once();
    secretManager.addKey(authKey2);
    expectLastCall().once();
    expect(secretManager.getCurrentKey()).andReturn(authKey2).once();

    replay(secretManager, zooDistributor);

    // Initialize the state from zookeeper
    keyManager.updateStateFromCurrentKeys();

    verify(secretManager, zooDistributor);

    assertEquals(authKey2.getKeyId(), keyManager.getIdSeq());
    assertEquals(authKey2.getCreationDate(), keyManager.getLastKeyUpdate());
  }
}
