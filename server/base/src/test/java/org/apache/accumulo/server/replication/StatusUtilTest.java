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
package org.apache.accumulo.server.replication;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.junit.Test;

public class StatusUtilTest {

  @Test
  public void newFileIsNotCompletelyReplicated() {
    assertFalse(StatusUtil.isSafeForRemoval(StatusUtil.fileCreated(0L)));
  }

  @Test
  public void openFileIsNotCompletelyReplicated() {
    assertFalse(StatusUtil.isSafeForRemoval(Status.newBuilder().setClosed(false).setBegin(0)
        .setEnd(1000).setInfiniteEnd(false).build()));
  }

  @Test
  public void closedFileWithDifferentBeginEndIsNotCompletelyReplicated() {
    assertFalse(StatusUtil.isSafeForRemoval(Status.newBuilder().setClosed(true).setBegin(0)
        .setEnd(1000).setInfiniteEnd(false).build()));
  }

  @Test
  public void closedFileWithInfEndAndNonMaxBeginIsNotCompletelyReplicated() {
    assertFalse(StatusUtil.isSafeForRemoval(
        Status.newBuilder().setClosed(true).setInfiniteEnd(true).setBegin(10000).build()));
  }

  @Test
  public void closedFileWithInfEndAndMaxBeginIsCompletelyReplicated() {
    assertTrue(StatusUtil.isSafeForRemoval(
        Status.newBuilder().setClosed(true).setInfiniteEnd(true).setBegin(Long.MAX_VALUE).build()));
  }

  @Test
  public void closeFileWithEqualBeginEndIsCompletelyReplicated() {
    assertTrue(StatusUtil.isSafeForRemoval(
        Status.newBuilder().setClosed(true).setEnd(100000).setBegin(100000).build()));
  }
}
