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
package org.apache.accumulo.core.clientImpl;

import static org.apache.accumulo.core.spi.scan.ConfigurableScanServerSelectorTest.nti;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.spi.scan.ScanServerAttempt;
import org.junit.jupiter.api.Test;

public class ScanAttemptsImplTest {

  private Map<TabletId,Collection<String>>
      simplify(Map<TabletId,Collection<ScanServerAttemptImpl>> map) {
    Map<TabletId,Collection<String>> ret = new HashMap<>();

    map.forEach((tabletId, scanAttempts) -> {
      Set<String> stringAttempts = new HashSet<>();
      scanAttempts.forEach(scanAttempt -> stringAttempts
          .add(scanAttempt.getServer() + "_" + scanAttempt.getResult()));
      ret.put(tabletId, stringAttempts);
    });

    return ret;
  }

  @Test
  public void testBasic() {
    ScanServerAttemptsImpl sai = new ScanServerAttemptsImpl();

    var snap1 = sai.snapshot();

    assertEquals(Map.of(), snap1);

    var tablet1 = nti("1", "a");

    var reporter1 = sai.createReporter("ss1:1", tablet1);

    reporter1.report(ScanServerAttempt.Result.BUSY);

    assertEquals(Map.of(), snap1);

    var snap2 = sai.snapshot();

    assertEquals(Map.of(tablet1, Set.of("ss1:1_BUSY")), simplify(snap2));

    reporter1.report(ScanServerAttempt.Result.ERROR);

    assertEquals(Map.of(), snap1);
    assertEquals(Map.of(tablet1, Set.of("ss1:1_BUSY")), simplify(snap2));

    var snap3 = sai.snapshot();

    assertEquals(Map.of(tablet1, Set.of("ss1:1_BUSY", "ss1:1_ERROR")), simplify(snap3));

    var tablet2 = nti("1", "m");
    var reporter2 = sai.createReporter("ss1:1", tablet2);
    var tablet3 = nti("2", "r");
    var reporter3 = sai.createReporter("ss2:2", tablet3);

    reporter2.report(ScanServerAttempt.Result.BUSY);
    reporter3.report(ScanServerAttempt.Result.ERROR);

    var snap4 = sai.snapshot();

    assertEquals(Map.of(), snap1);
    assertEquals(Map.of(tablet1, Set.of("ss1:1_BUSY")), simplify(snap2));
    assertEquals(Map.of(tablet1, Set.of("ss1:1_BUSY", "ss1:1_ERROR")), simplify(snap3));
    assertEquals(Map.of(tablet1, Set.of("ss1:1_BUSY", "ss1:1_ERROR"), tablet2, Set.of("ss1:1_BUSY"),
        tablet3, Set.of("ss2:2_ERROR")), simplify(snap4));

  }
}
