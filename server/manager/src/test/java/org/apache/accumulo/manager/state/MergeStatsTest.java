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
package org.apache.accumulo.manager.state;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import java.util.List;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.TabletLocationState;
import org.apache.accumulo.core.metadata.TabletLocationState.BadLocationStateException;
import org.apache.accumulo.server.manager.state.MergeInfo;
import org.apache.accumulo.server.manager.state.MergeInfo.Operation;
import org.apache.accumulo.server.manager.state.MergeState;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class MergeStatsTest {

  @Test
  public void testVerifyState() {
    KeyExtent keyExtent = new KeyExtent(TableId.of("table"), new Text("end"), new Text("begin"));
    MergeInfo mergeInfo = new MergeInfo(keyExtent, Operation.MERGE);
    MergeStats stats = new MergeStats(mergeInfo);
    mergeInfo.setState(MergeState.WAITING_FOR_OFFLINE);

    // Verify WAITING_FOR_OFFLINE does not throw an exception
    stats.verifyState(mergeInfo, MergeState.WAITING_FOR_OFFLINE);

    // State is wrong so should throw exception
    mergeInfo.setState(MergeState.WAITING_FOR_CHOPPED);
    assertThrows(IllegalStateException.class,
        () -> stats.verifyState(mergeInfo, MergeState.WAITING_FOR_OFFLINE));
  }

  @Test
  public void testVerifyWalogs() throws BadLocationStateException {
    KeyExtent keyExtent = new KeyExtent(TableId.of("table"), new Text("end"), new Text("begin"));
    MergeStats stats = new MergeStats(new MergeInfo(keyExtent, Operation.MERGE));

    // Verify that if there are Walogs the return true, else false
    assertTrue(stats.verifyWalogs(getState(keyExtent, List.of())));
    assertFalse(stats.verifyWalogs(getState(keyExtent, List.of(List.of("log1")))));
  }

  private TabletLocationState getState(KeyExtent keyExtent, Collection<Collection<String>> walogs)
      throws BadLocationStateException {
    return new TabletLocationState(keyExtent, null, null, null, null, walogs, true);
  }

}
