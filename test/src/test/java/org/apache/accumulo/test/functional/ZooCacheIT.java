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
package org.apache.accumulo.test.functional;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.BeforeClass;
import org.junit.Test;

public class ZooCacheIT extends ConfigurableMacIT {

  @Override
  protected int defaultTimeoutSeconds() {
    return 2 * 60;
  }

  private static String pathName = "/zcTest-42";
  private static File testDir;

  @BeforeClass
  public static void createTestDirectory() {
    testDir = createSharedTestDir(ZooCacheIT.class.getName() + pathName);
  }

  @Test
  public void test() throws Exception {
    assertEquals(0, exec(CacheTestClean.class, pathName, testDir.getAbsolutePath()).waitFor());
    final AtomicReference<Exception> ref = new AtomicReference<Exception>();
    List<Thread> threads = new ArrayList<Thread>();
    for (int i = 0; i < 3; i++) {
      Thread reader = new Thread() {
        @Override
        public void run() {
          try {
            CacheTestReader.main(new String[] {pathName, testDir.getAbsolutePath(), getConnector().getInstance().getZooKeepers()});
          } catch (Exception ex) {
            ref.set(ex);
          }
        }
      };
      reader.start();
      threads.add(reader);
    }
    assertEquals(0, exec(CacheTestWriter.class, pathName, testDir.getAbsolutePath(), "3", "50").waitFor());
    for (Thread t : threads) {
      t.join();
      if (ref.get() != null)
        throw ref.get();
    }
  }

}
