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
package org.apache.accumulo.test.functional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.clientImpl.ClientInfo;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class ZooCacheIT extends ConfigurableMacBase {

  private static String pathName = "/zcTest-42";
  private static File testDir;

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "path provided by test")
  @BeforeAll
  public static void createTestDirectory() {
    testDir = new File(createTestDir(ZooCacheIT.class.getName()), pathName);
    FileUtils.deleteQuietly(testDir);
    assertTrue(testDir.mkdir());
  }

  @Test
  public void test() throws Exception {
    assertEquals(0, exec(CacheTestClean.class, pathName, testDir.getAbsolutePath()).waitFor());
    final AtomicReference<Exception> ref = new AtomicReference<>();
    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      Thread reader = new Thread(() -> {
        try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
          CacheTestReader.main(new String[] {pathName, testDir.getAbsolutePath(),
              ClientInfo.from(client.properties()).getZooKeepers()});
        } catch (Exception ex) {
          ref.set(ex);
        }
      });
      reader.start();
      threads.add(reader);
    }
    assertEquals(0,
        exec(CacheTestWriter.class, pathName, testDir.getAbsolutePath(), "3", "50").waitFor());
    for (Thread t : threads) {
      t.join();
      if (ref.get() != null) {
        throw ref.get();
      }
    }
  }

}
