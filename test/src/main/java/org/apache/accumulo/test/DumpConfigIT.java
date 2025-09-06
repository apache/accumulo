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
package org.apache.accumulo.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.util.Admin;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.test.functional.FunctionalTestUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class DumpConfigIT extends ConfigurableMacBase {

  @TempDir
  private static Path tempDir;

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.getClusterServerConfiguration().addCompactorResourceGroup("test", 1);
    cfg.setSiteConfig(Collections.singletonMap(Property.TABLE_FILE_BLOCK_SIZE.getKey(), "1234567"));
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "user.dir is suitable test input")
  @Test
  public void test() throws Exception {

    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      client.resourceGroupOperations().setProperty(ResourceGroupId.of("test"),
          Property.COMPACTION_WARN_TIME.getKey(), "3m");
    }

    Path folder = tempDir.resolve(testName());
    if (!Files.isDirectory(folder)) {
      Files.createDirectories(folder);
    }
    Path siteFileBackup = folder.resolve("accumulo.properties.bak");
    assertFalse(Files.exists(siteFileBackup));
    assertEquals(0, exec(Admin.class, "dumpConfig", "-a", "-d", folder.toString()).waitFor());
    assertTrue(Files.exists(siteFileBackup));
    String site = FunctionalTestUtils.readAll(Files.newInputStream(siteFileBackup));
    assertTrue(site.contains(Property.TABLE_FILE_BLOCK_SIZE.getKey()));
    assertTrue(site.contains("1234567"));
    String meta = FunctionalTestUtils
        .readAll(Files.newInputStream(folder.resolve(SystemTables.METADATA.tableName() + ".cfg")));
    assertTrue(meta.contains(Property.TABLE_FILE_REPLICATION.getKey()));
    String systemPerm =
        FunctionalTestUtils.readAll(Files.newInputStream(folder.resolve("root_user.cfg")));
    assertTrue(systemPerm.contains("grant System.ALTER_USER -s -u root"));
    assertTrue(systemPerm
        .contains("grant Table.READ -t " + SystemTables.METADATA.tableName() + " -u root"));
    assertFalse(systemPerm
        .contains("grant Table.DROP -t " + SystemTables.METADATA.tableName() + " -u root"));
    String rg = FunctionalTestUtils.readAll(Files.newInputStream(folder.resolve("test_rg.cfg")));
    assertTrue(rg.contains("createresourcegroup test"));
    assertTrue(rg.contains("config -rg test -s " + Property.COMPACTION_WARN_TIME.getKey() + "=3m"));

  }
}
