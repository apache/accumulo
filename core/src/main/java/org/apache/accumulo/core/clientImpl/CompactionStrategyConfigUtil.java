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

import java.io.DataInput;
import java.io.DataOutput;
import java.util.Map;

import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.CompactionStrategyConfig;

@SuppressWarnings("removal")
public class CompactionStrategyConfigUtil {

  public static final CompactionStrategyConfig DEFAULT_STRATEGY = new CompactionStrategyConfig("") {
    @Override
    public CompactionStrategyConfig setOptions(Map<String,String> opts) {
      throw new UnsupportedOperationException();
    }
  };

  private static final int MAGIC = 0xcc5e6024;

  public static void encode(DataOutput dout, CompactionConfig cc) {
    var cs = cc.getCompactionStrategy();
    UserCompactionUtils.encode(dout, MAGIC, 1, cs.getClassName(), cs.getOptions());
  }

  public static void decode(CompactionConfig cc, DataInput din) {
    var pcd = UserCompactionUtils.decode(din, MAGIC, 1);
    var csc = new CompactionStrategyConfig(pcd.className).setOptions(pcd.opts);

    if (!isDefault(csc)) {
      cc.setCompactionStrategy(csc);
    }
  }

  public static boolean isDefault(CompactionStrategyConfig compactionStrategy) {
    return compactionStrategy.equals(DEFAULT_STRATEGY);

  }

  public static boolean isDefault(CompactionConfig compactionConfig) {
    return isDefault(compactionConfig.getCompactionStrategy());
  }
}
