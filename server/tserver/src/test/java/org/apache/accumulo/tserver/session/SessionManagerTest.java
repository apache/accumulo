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
package org.apache.accumulo.tserver.session;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.server.ServerContext;
import org.junit.jupiter.api.Test;

public class SessionManagerTest {

  private static class TestSession extends Session {

    int cleanupCount;

    TestSession(int cleanupCount) {
      super(null);
      this.cleanupCount = cleanupCount;
    }

    @Override
    public boolean cleanup() {
      return cleanupCount-- <= 0;
    }
  }

  @Test
  public void testTestcode() {
    // test behavior of test class
    TestSession session = new TestSession(2);
    assertFalse(session.cleanup());
    assertFalse(session.cleanup());
    assertTrue(session.cleanup());
    assertTrue(session.cleanup());
  }

  @Test
  public void testFullDeferredCleanupQueue() {
    ArrayBlockingQueue<Session> deferredCleanupQeue = new ArrayBlockingQueue<>(3);

    deferredCleanupQeue.add(new TestSession(2));
    deferredCleanupQeue.add(new TestSession(2));
    deferredCleanupQeue.add(new TestSession(2));

    // the queue is full, so cleanup method should repeatedly call cleanup instead of queuing
    TestSession session = new TestSession(5);
    SessionManager.cleanup(deferredCleanupQeue, session);
    assertEquals(-1, session.cleanupCount);
    assertEquals(3, deferredCleanupQeue.size());
    assertTrue(deferredCleanupQeue.stream().allMatch(s -> ((TestSession) s).cleanupCount == 2));
  }

  @Test
  public void testDefersCleanup() {
    ArrayBlockingQueue<Session> deferredCleanupQeue = new ArrayBlockingQueue<>(3);

    deferredCleanupQeue.add(new TestSession(2));
    deferredCleanupQeue.add(new TestSession(2));

    TestSession session = new TestSession(5);

    // the queue is not full so expect the session to be queued after cleanup
    SessionManager.cleanup(deferredCleanupQeue, session);

    assertEquals(4, session.cleanupCount);
    assertEquals(3, deferredCleanupQeue.size());
    assertEquals(2,
        deferredCleanupQeue.stream().filter(s -> ((TestSession) s).cleanupCount == 2).count());
    assertEquals(1,
        deferredCleanupQeue.stream().filter(s -> ((TestSession) s).cleanupCount == 4).count());
  }

  @Test
  public void testDeferNotNeeded() {
    ArrayBlockingQueue<Session> deferredCleanupQeue = new ArrayBlockingQueue<>(3);

    deferredCleanupQeue.add(new TestSession(2));
    deferredCleanupQeue.add(new TestSession(2));

    TestSession session = new TestSession(0);

    // the queue is not full, but the session will cleanup in one call so it should not be queued
    SessionManager.cleanup(deferredCleanupQeue, session);

    assertEquals(-1, session.cleanupCount);
    assertEquals(2, deferredCleanupQeue.size());
    assertEquals(2,
        deferredCleanupQeue.stream().filter(s -> ((TestSession) s).cleanupCount == 2).count());
  }

  @Test
  public void testDisallowNewReservation() {
    var sessionManager = createSessionManager();

    var sid = sessionManager.createSession(new TestSession(2), true);

    // this should prevent future reservation and return false because its currently reserved
    assertFalse(sessionManager.disallowNewReservations(sid));

    // should not have a problem un-reserving
    sessionManager.unreserveSession(sid);

    // should not be able to reserve the session because reservations were disabled
    assertNull(sessionManager.reserveSession(sid));
    assertNull(sessionManager.reserveSession(sid, false));

    // should return true now that its not reserved
    assertTrue(sessionManager.disallowNewReservations(sid));

    sessionManager.removeSession(sid);

    // should return true for nonexistent session
    assertTrue(sessionManager.disallowNewReservations(sid));
  }

  private SessionManager createSessionManager() {
    ServerContext ctx = createMock(ServerContext.class);
    expect(ctx.getConfiguration()).andReturn(DefaultConfiguration.getInstance()).anyTimes();
    var executor = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1);
    expect(ctx.getScheduledExecutor()).andReturn(executor).anyTimes();
    replay(ctx);
    return new SessionManager(ctx);
  }
}
