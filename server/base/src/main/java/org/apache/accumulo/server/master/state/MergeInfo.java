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
package org.apache.accumulo.server.master.state;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.hadoop.io.Writable;

/**
 * Information about the current merge/rangeDelete.
 *
 * Writable to serialize for zookeeper and the Tablet
 */
public class MergeInfo implements Writable {

  public enum Operation {
    MERGE, DELETE,
  }

  MergeState state = MergeState.NONE;
  KeyExtent extent;
  Operation operation = Operation.MERGE;

  public MergeInfo() {}

  @Override
  public void readFields(DataInput in) throws IOException {
    extent = new KeyExtent();
    extent.readFields(in);
    state = MergeState.values()[in.readInt()];
    operation = Operation.values()[in.readInt()];
  }

  @Override
  public void write(DataOutput out) throws IOException {
    extent.write(out);
    out.writeInt(state.ordinal());
    out.writeInt(operation.ordinal());
  }

  public MergeInfo(KeyExtent range, Operation op) {
    this.extent = range;
    this.operation = op;
  }

  public MergeState getState() {
    return state;
  }

  public KeyExtent getExtent() {
    return extent;
  }

  public Operation getOperation() {
    return operation;
  }

  public void setState(MergeState state) {
    this.state = state;
  }

  public boolean isDelete() {
    return this.operation.equals(Operation.DELETE);
  }

  public boolean needsToBeChopped(KeyExtent otherExtent) {
    // During a delete, the block after the merge will be stretched to cover the deleted area.
    // Therefore, it needs to be chopped
    if (!otherExtent.getTableId().equals(extent.getTableId()))
      return false;
    if (isDelete())
      return otherExtent.getPrevEndRow() != null && otherExtent.getPrevEndRow().equals(extent.getEndRow());
    else
      return this.extent.overlaps(otherExtent);
  }

  public boolean overlaps(KeyExtent otherExtent) {
    boolean result = this.extent.overlaps(otherExtent);
    if (!result && needsToBeChopped(otherExtent))
      return true;
    return result;
  }

  @Override
  public String toString() {
    if (!state.equals(MergeState.NONE))
      return "Merge " + operation.toString() + " of " + extent + " State: " + state;
    return "No Merge in progress";
  }
}
