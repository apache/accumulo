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
package org.apache.accumulo.tserver;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.clientImpl.CompressedIterators;
import org.apache.accumulo.core.clientImpl.CompressedIterators.IterConfig;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.thrift.IterInfo;
import org.apache.accumulo.core.dataImpl.thrift.TCMResult;
import org.apache.accumulo.core.dataImpl.thrift.TCMStatus;
import org.apache.accumulo.core.dataImpl.thrift.TCondition;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.IteratorBuilder;
import org.apache.accumulo.core.iteratorsImpl.IteratorConfigUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.conf.TableConfiguration.ParsedIteratorConfig;
import org.apache.accumulo.server.iterators.TabletIteratorEnvironment;
import org.apache.accumulo.tserver.data.ServerConditionalMutation;
import org.apache.hadoop.io.Text;

public class ConditionCheckerContext {
  private CompressedIterators compressedIters;

  private List<IterInfo> tableIters;
  private Map<String,Map<String,String>> tableIterOpts;
  private TabletIteratorEnvironment tie;
  private String context;

  private static class MergedIterConfig {
    List<IterInfo> mergedIters;
    Map<String,Map<String,String>> mergedItersOpts;

    MergedIterConfig(List<IterInfo> mergedIters, Map<String,Map<String,String>> mergedItersOpts) {
      this.mergedIters = mergedIters;
      this.mergedItersOpts = mergedItersOpts;
    }
  }

  private Map<ByteSequence,MergedIterConfig> mergedIterCache = new HashMap<>();

  ConditionCheckerContext(ServerContext context, CompressedIterators compressedIters,
      TableConfiguration tableConf) {
    this.compressedIters = compressedIters;

    ParsedIteratorConfig pic = tableConf.getParsedIteratorConfig(IteratorScope.scan);

    tableIters = pic.getIterInfo();
    tableIterOpts = pic.getOpts();
    this.context = pic.getServiceEnv();

    tie = new TabletIteratorEnvironment(context, IteratorScope.scan, tableConf,
        tableConf.getTableId());
  }

  SortedKeyValueIterator<Key,Value> buildIterator(SortedKeyValueIterator<Key,Value> systemIter,
      TCondition tc) throws IOException {

    ArrayByteSequence key = new ArrayByteSequence(tc.iterators);
    MergedIterConfig mic = mergedIterCache.get(key);
    if (mic == null) {
      IterConfig ic = compressedIters.decompress(tc.iterators);

      List<IterInfo> mergedIters = new ArrayList<>(tableIters.size() + ic.ssiList.size());
      Map<String,Map<String,String>> mergedItersOpts =
          new HashMap<>(tableIterOpts.size() + ic.ssio.size());

      IteratorConfigUtil.mergeIteratorConfig(mergedIters, mergedItersOpts, tableIters,
          tableIterOpts, ic.ssiList, ic.ssio);

      mic = new MergedIterConfig(mergedIters, mergedItersOpts);

      mergedIterCache.put(key, mic);
    }

    var iteratorBuilder = IteratorBuilder.builder(mic.mergedIters).opts(mic.mergedItersOpts)
        .env(tie).useClassLoader(context).useClassCache(true).build();
    return IteratorConfigUtil.loadIterators(systemIter, iteratorBuilder);
  }

  boolean checkConditions(SortedKeyValueIterator<Key,Value> systemIter,
      ServerConditionalMutation scm) throws IOException {
    boolean add = true;

    for (TCondition tc : scm.getConditions()) {

      Range range;
      if (tc.hasTimestamp) {
        range = Range.exact(new Text(scm.getRow()), new Text(tc.getCf()), new Text(tc.getCq()),
            new Text(tc.getCv()), tc.getTs());
      } else {
        range = Range.exact(new Text(scm.getRow()), new Text(tc.getCf()), new Text(tc.getCq()),
            new Text(tc.getCv()));
      }

      SortedKeyValueIterator<Key,Value> iter = buildIterator(systemIter, tc);

      ByteSequence cf = new ArrayByteSequence(tc.getCf());
      iter.seek(range, Collections.singleton(cf), true);
      Value val = null;
      if (iter.hasTop()) {
        val = iter.getTopValue();
      }

      if ((val == null ^ tc.getVal() == null)
          || (val != null && !Arrays.equals(tc.getVal(), val.get()))) {
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

    public ConditionChecker(List<ServerConditionalMutation> conditionsToCheck,
        List<ServerConditionalMutation> okMutations, List<TCMResult> results) {
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

  public ConditionChecker newChecker(List<ServerConditionalMutation> conditionsToCheck,
      List<ServerConditionalMutation> okMutations, List<TCMResult> results) {
    return new ConditionChecker(conditionsToCheck, okMutations, results);
  }
}
