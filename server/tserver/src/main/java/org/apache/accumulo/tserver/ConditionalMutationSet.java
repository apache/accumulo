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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.tserver.data.ServerConditionalMutation;
import org.apache.hadoop.io.WritableComparator;

/**
 *
 */
public class ConditionalMutationSet {

  interface DeferFilter {
    void defer(List<ServerConditionalMutation> scml, List<ServerConditionalMutation> okMutations, List<ServerConditionalMutation> deferred);
  }

  static class DuplicateFilter implements DeferFilter {
    public void defer(List<ServerConditionalMutation> scml, List<ServerConditionalMutation> okMutations, List<ServerConditionalMutation> deferred) {
      okMutations.add(scml.get(0));
      for (int i = 1; i < scml.size(); i++) {
        if (Arrays.equals(scml.get(i - 1).getRow(), scml.get(i).getRow())) {
          deferred.add(scml.get(i));
        } else {
          okMutations.add(scml.get(i));
        }
      }
    }
  }

  static void defer(Map<KeyExtent,List<ServerConditionalMutation>> updates, Map<KeyExtent,List<ServerConditionalMutation>> deferredMutations, DeferFilter filter) {
    for (Entry<KeyExtent,List<ServerConditionalMutation>> entry : updates.entrySet()) {
      List<ServerConditionalMutation> scml = entry.getValue();
      List<ServerConditionalMutation> okMutations = new ArrayList<ServerConditionalMutation>(scml.size());
      List<ServerConditionalMutation> deferred = new ArrayList<ServerConditionalMutation>();
      filter.defer(scml, okMutations, deferred);

      if (deferred.size() > 0) {
        scml.clear();
        scml.addAll(okMutations);
        List<ServerConditionalMutation> l = deferredMutations.get(entry.getKey());
        if (l == null) {
          l = deferred;
          deferredMutations.put(entry.getKey(), l);
        } else {
          l.addAll(deferred);
        }

      }
    }
  }

  static void deferDuplicatesRows(Map<KeyExtent,List<ServerConditionalMutation>> updates, Map<KeyExtent,List<ServerConditionalMutation>> deferred) {
    defer(updates, deferred, new DuplicateFilter());
  }

  static void sortConditionalMutations(Map<KeyExtent,List<ServerConditionalMutation>> updates) {
    for (Entry<KeyExtent,List<ServerConditionalMutation>> entry : updates.entrySet()) {
      Collections.sort(entry.getValue(), new Comparator<ServerConditionalMutation>() {
        @Override
        public int compare(ServerConditionalMutation o1, ServerConditionalMutation o2) {
          return WritableComparator.compareBytes(o1.getRow(), 0, o1.getRow().length, o2.getRow(), 0, o2.getRow().length);
        }
      });
    }
  }
}
