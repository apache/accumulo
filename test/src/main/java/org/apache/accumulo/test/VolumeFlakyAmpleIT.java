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

import java.util.TreeSet;

import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.ample.FlakyAmpleManager;
import org.apache.accumulo.test.ample.FlakyAmpleTserver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

public class VolumeFlakyAmpleIT extends VolumeITBase {
  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    super.configure(cfg, hadoopCoreSite);
    cfg.setServerClass(ServerType.MANAGER, FlakyAmpleManager.class);
    cfg.setServerClass(ServerType.TABLET_SERVER, FlakyAmpleTserver.class);
    // The test creates a lots of tablet that need to compact. Reserving and commiting compactions
    // is slower because of FlakyAmple causing conditional mutations to fail. So start more
    // compactors to compensate for this.
    cfg.getClusterServerConfiguration().setNumDefaultCompactors(3);
  }

  @Override
  protected TreeSet<Text> generateSplits() {
    // The regular version of this test creates 100 tablets. However 100 tablets and FlakyAmple
    // causing each tablet operation take longer results in longer test runs times. So lower the
    // number of tablets to 10 to speed up the test with flaky ample.
    TreeSet<Text> splits = new TreeSet<>();
    for (int i = 10; i < 100; i += 10) {
      splits.add(new Text(String.format("%06d", i * 100)));
    }
    return splits;
  }
}
