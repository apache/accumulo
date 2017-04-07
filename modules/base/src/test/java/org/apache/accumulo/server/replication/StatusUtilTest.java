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
package org.apache.accumulo.server.replication;

import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.junit.Assert;
import org.junit.Test;

public class StatusUtilTest {

  @Test
  public void newFileIsNotCompletelyReplicated() {
    Assert.assertFalse(StatusUtil.isSafeForRemoval(StatusUtil.fileCreated(0l)));
  }

  @Test
  public void openFileIsNotCompletelyReplicated() {
    Assert.assertFalse(StatusUtil.isSafeForRemoval(Status.newBuilder().setClosed(false).setBegin(0).setEnd(1000).setInfiniteEnd(false).build()));
  }

  @Test
  public void closedFileWithDifferentBeginEndIsNotCompletelyReplicated() {
    Assert.assertFalse(StatusUtil.isSafeForRemoval(Status.newBuilder().setClosed(true).setBegin(0).setEnd(1000).setInfiniteEnd(false).build()));
  }

  @Test
  public void closedFileWithInfEndAndNonMaxBeginIsNotCompletelyReplicated() {
    Assert.assertFalse(StatusUtil.isSafeForRemoval(Status.newBuilder().setClosed(true).setInfiniteEnd(true).setBegin(10000).build()));
  }

  @Test
  public void closedFileWithInfEndAndMaxBeginIsCompletelyReplicated() {
    Assert.assertTrue(StatusUtil.isSafeForRemoval(Status.newBuilder().setClosed(true).setInfiniteEnd(true).setBegin(Long.MAX_VALUE).build()));
  }

  @Test
  public void closeFileWithEqualBeginEndIsCompletelyReplicated() {
    Assert.assertTrue(StatusUtil.isSafeForRemoval(Status.newBuilder().setClosed(true).setEnd(100000).setBegin(100000).build()));
  }
}
