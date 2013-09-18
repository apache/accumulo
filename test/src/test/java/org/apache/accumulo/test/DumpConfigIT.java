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
package org.apache.accumulo.test;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.util.Collections;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.accumulo.server.util.Admin;
import org.apache.accumulo.test.functional.ConfigurableMacIT;
import org.apache.accumulo.test.functional.FunctionalTestUtils;
import org.junit.Test;

public class DumpConfigIT extends ConfigurableMacIT {

  @Override
  public void configure(MiniAccumuloConfig cfg) {
    cfg.setSiteConfig(Collections.singletonMap(Property.TABLE_FILE_BLOCK_SIZE.getKey(), "1234567"));
  }
  
  @Test(timeout=2 * 60 * 1000)
  public void test() throws Exception {
    File siteFile = new File("target/accumulo-site.xml.bak");
    siteFile.delete();
    assertEquals(0, exec(Admin.class, new String[]{"dumpConfig", "-a", "-d", "target"}).waitFor());
    assertTrue(siteFile.exists());
    String site = FunctionalTestUtils.readAll(new FileInputStream("target/accumulo-site.xml.bak"));
    assertTrue(site.contains(Property.TABLE_FILE_BLOCK_SIZE.getKey()));
    assertTrue(site.contains("1234567"));
    String meta = FunctionalTestUtils.readAll(new FileInputStream("target/!METADATA.cfg"));
    assertTrue(meta.contains(Property.TABLE_FILE_REPLICATION.getKey()));
  }
  
}
