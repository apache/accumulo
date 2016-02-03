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

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.impl.CompressedIterators;
import org.apache.accumulo.core.client.impl.CompressedIterators.IterConfig;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.thrift.IterInfo;
import org.apache.accumulo.core.data.thrift.TCMResult;
import org.apache.accumulo.core.data.thrift.TCMStatus;
import org.apache.accumulo.core.data.thrift.TCondition;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.tserver.data.ServerConditionalMutation;
import org.apache.hadoop.io.Text;

public class ConditionCheckerContext {
  private CompressedIterators compressedIters;

  private List<IterInfo> tableIters;
  private Map<String,Map<String,String>> tableIterOpts;
  private TabletIteratorEnvironment tie;
  private String context;
  private Map<String,Class<? extends SortedKeyValueIterator<Key,Value>>> classCache;

  private static class MergedIterConfig {
    List<IterInfo> mergedIters;
    Map<String,Map<String,String>> mergedItersOpts;

    MergedIterConfig(List<IterInfo> mergedIters, Map<String,Map<String,String>> mergedItersOpts) {
      this.mergedIters = mergedIters;
      this.mergedItersOpts = mergedItersOpts;
    }
  }

  private Map<ByteSequence,MergedIterConfig> mergedIterCache = new HashMap<>();

  ConditionCheckerContext(CompressedIterators compressedIters, AccumuloConfiguration tableConf) {
    this.compressedIters = compressedIters;

    tableIters = new ArrayList<>();
    tableIterOpts = new HashMap<>();

    // parse table iterator config once
    IteratorUtil.parseIterConf(IteratorScope.scan, tableIters, tableIterOpts, tableConf);

    context = tableConf.get(Property.TABLE_CLASSPATH);

    classCache = new HashMap<>();

    tie = new TabletIteratorEnvironment(IteratorScope.scan, tableConf);
  }

  SortedKeyValueIterator<Key,Value> buildIterator(SortedKeyValueIterator<Key,Value> systemIter, TCondition tc) throws IOException {

    ArrayByteSequence key = new ArrayByteSequence(tc.iterators);
    MergedIterConfig mic = mergedIterCache.get(key);
    if (mic == null) {
      IterConfig ic = compressedIters.decompress(tc.iterators);

      List<IterInfo> mergedIters = new ArrayList<>(tableIters.size() + ic.ssiList.size());
      Map<String,Map<String,String>> mergedItersOpts = new HashMap<>(tableIterOpts.size() + ic.ssio.size());

      IteratorUtil.mergeIteratorConfig(mergedIters, mergedItersOpts, tableIters, tableIterOpts, ic.ssiList, ic.ssio);

      mic = new MergedIterConfig(mergedIters, mergedItersOpts);

      mergedIterCache.put(key, mic);
    }

    return IteratorUtil.loadIterators(systemIter, mic.mergedIters, mic.mergedItersOpts, tie, true, context, classCache);
  }

  boolean checkConditions(SortedKeyValueIterator<Key,Value> systemIter, ServerConditionalMutation scm) throws IOException {
    boolean add = true;

    for (TCondition tc : scm.getConditions()) {

      Range range;
      if (tc.hasTimestamp)
        range = Range.exact(new Text(scm.getRow()), new Text(tc.getCf()), new Text(tc.getCq()), new Text(tc.getCv()), tc.getTs());
      else
        range = Range.exact(new Text(scm.getRow()), new Text(tc.getCf()), new Text(tc.getCq()), new Text(tc.getCv()));

      SortedKeyValueIterator<Key,Value> iter = buildIterator(systemIter, tc);

      ByteSequence cf = new ArrayByteSequence(tc.getCf());
      iter.seek(range, Collections.singleton(cf), true);
      Value val = null;
      if (iter.hasTop()) {
        val = iter.getTopValue();
      }

      if ((val == null ^ tc.getVal() == null) || (val != null && !Arrays.equals(tc.getVal(), val.get()))) {
        add = false;
        break;
      }
    }
    return add;
  }

  public class ConditionChecker {

    private List<ServerConditionalMutation> conditionsToCheck;
    private List<ServerConditionalMutation> okMutations;
    private List<TCMResult> results;
    private boolean checked = false;

    public ConditionChecker(List<ServerConditionalMutation> conditionsToCheck, List<ServerConditionalMutation> okMutations, List<TCMResult> results) {
      this.conditionsToCheck = conditionsToCheck;
      this.okMutations = okMutations;
      this.results = results;
    }

    public void check(SortedKeyValueIterator<Key,Value> systemIter) throws IOException {
      checkArgument(!checked, "check() method should only be called once");
      checked = true;

      for (ServerConditionalMutation scm : conditionsToCheck) {
        if (checkConditions(systemIter, scm)) {
          okMutations.add(scm);
        } else {
          results.add(new TCMResult(scm.getID(), TCMStatus.REJECTED));
        }
      }
    }
  }

  public ConditionChecker newChecker(List<ServerConditionalMutation> conditionsToCheck, List<ServerConditionalMutation> okMutations, List<TCMResult> results) {
    return new ConditionChecker(conditionsToCheck, okMutations, results);
  }
}
