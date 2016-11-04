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
package org.apache.accumulo.server.master.tableOps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.CompactionStrategyConfig;
import org.apache.accumulo.core.client.impl.CompactionStrategyConfigUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class UserCompactionConfig implements Writable {
  byte[] startRow;
  byte[] endRow;
  List<IteratorSetting> iterators;
  private CompactionStrategyConfig compactionStrategy;

  public UserCompactionConfig(byte[] startRow, byte[] endRow, List<IteratorSetting> iterators, CompactionStrategyConfig csc) {
    this.startRow = startRow;
    this.endRow = endRow;
    this.iterators = iterators;
    this.compactionStrategy = csc;
  }

  public UserCompactionConfig() {
    startRow = null;
    endRow = null;
    iterators = Collections.emptyList();
    compactionStrategy = CompactionStrategyConfigUtil.DEFAULT_STRATEGY;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeBoolean(startRow != null);
    if (startRow != null) {
      out.writeInt(startRow.length);
      out.write(startRow);
    }

    out.writeBoolean(endRow != null);
    if (endRow != null) {
      out.writeInt(endRow.length);
      out.write(endRow);
    }

    out.writeInt(iterators.size());
    for (IteratorSetting is : iterators) {
      is.write(out);
    }

    CompactionStrategyConfigUtil.encode(out, compactionStrategy);

  }

  @Override
  public void readFields(DataInput in) throws IOException {
    if (in.readBoolean()) {
      startRow = new byte[in.readInt()];
      in.readFully(startRow);
    } else {
      startRow = null;
    }

    if (in.readBoolean()) {
      endRow = new byte[in.readInt()];
      in.readFully(endRow);
    } else {
      endRow = null;
    }

    int num = in.readInt();
    iterators = new ArrayList<>(num);

    for (int i = 0; i < num; i++) {
      iterators.add(new IteratorSetting(in));
    }

    compactionStrategy = CompactionStrategyConfigUtil.decode(in);
  }

  public Text getEndRow() {
    if (endRow == null)
      return null;
    return new Text(endRow);
  }

  public Text getStartRow() {
    if (startRow == null)
      return null;
    return new Text(startRow);
  }

  public List<IteratorSetting> getIterators() {
    return iterators;
  }

  public CompactionStrategyConfig getCompactionStrategy() {
    return compactionStrategy;
  }
}
