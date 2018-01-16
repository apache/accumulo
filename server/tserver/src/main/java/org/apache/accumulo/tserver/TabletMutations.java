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
package org.apache.accumulo.tserver;

import java.util.List;

import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.data.Mutation;

public class TabletMutations {
  private final int tid;
  private final long seq;
  private final List<Mutation> mutations;
  private final Durability durability;

  public TabletMutations(int tid, long seq, List<Mutation> mutations, Durability durability) {
    this.tid = tid;
    this.seq = seq;
    this.mutations = mutations;
    this.durability = durability;
  }

  public List<Mutation> getMutations() {
    return mutations;
  }

  public int getTid() {
    return tid;
  }

  public long getSeq() {
    return seq;
  }

  public Durability getDurability() {
    return durability;
  }

}
