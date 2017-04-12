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
package org.apache.accumulo.server.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.IteratorSetting.Column;
import org.apache.accumulo.core.client.impl.BaseIteratorEnvironment;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.DevNull;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class StatusCombinerTest {

  private StatusCombiner combiner;
  private Key key;
  private Status.Builder builder;

  private static class TestIE extends BaseIteratorEnvironment {
    @Override
    public IteratorScope getIteratorScope() {
      return IteratorScope.scan;
    }
  }

  @Before
  public void initCombiner() throws IOException {
    key = new Key();
    combiner = new StatusCombiner();
    builder = Status.newBuilder();
    IteratorSetting cfg = new IteratorSetting(50, StatusCombiner.class);
    Combiner.setColumns(cfg, Collections.singletonList(new Column(StatusSection.NAME)));
    combiner.init(new DevNull(), cfg.getOptions(), new TestIE());
  }

  @Test
  public void returnsSameObject() {
    Status status = StatusUtil.ingestedUntil(10);
    // When combining only one message, we should get back the same instance
    Status ret = combiner.typedReduce(key, Collections.singleton(status).iterator());
    Assert.assertEquals(status, ret);
    Assert.assertTrue(status == ret);
  }

  @Test
  public void newStatusWithNewIngest() {
    Status orig = StatusUtil.fileCreated(100);
    Status status = StatusUtil.replicatedAndIngested(10, 20);
    Status ret = combiner.typedReduce(key, Arrays.asList(orig, status).iterator());
    Assert.assertEquals(10l, ret.getBegin());
    Assert.assertEquals(20l, ret.getEnd());
    Assert.assertEquals(100l, ret.getCreatedTime());
    Assert.assertEquals(false, ret.getClosed());
  }

  @Test
  public void newStatusWithNewIngestSingleBuilder() {
    Status orig = StatusUtil.fileCreated(100);
    Status status = StatusUtil.replicatedAndIngested(builder, 10, 20);
    Status ret = combiner.typedReduce(key, Arrays.asList(orig, status).iterator());
    Assert.assertEquals(10l, ret.getBegin());
    Assert.assertEquals(20l, ret.getEnd());
    Assert.assertEquals(100l, ret.getCreatedTime());
    Assert.assertEquals(false, ret.getClosed());
  }

  @Test
  public void commutativeNewFile() {
    Status newFile = StatusUtil.fileCreated(100), firstSync = StatusUtil.ingestedUntil(100), secondSync = StatusUtil.ingestedUntil(200);

    Status order1 = combiner.typedReduce(key, Arrays.asList(newFile, firstSync, secondSync).iterator()), order2 = combiner.typedReduce(key,
        Arrays.asList(secondSync, firstSync, newFile).iterator());

    Assert.assertEquals(order1, order2);
  }

  @Test
  public void commutativeNewFileSingleBuilder() {
    Status newFile = StatusUtil.fileCreated(100), firstSync = StatusUtil.ingestedUntil(builder, 100), secondSync = StatusUtil.ingestedUntil(builder, 200);

    Status order1 = combiner.typedReduce(key, Arrays.asList(newFile, firstSync, secondSync).iterator()), order2 = combiner.typedReduce(key,
        Arrays.asList(secondSync, firstSync, newFile).iterator());

    Assert.assertEquals(order1, order2);
  }

  @Test
  public void commutativeNewUpdates() {
    Status newFile = StatusUtil.fileCreated(100), firstSync = StatusUtil.ingestedUntil(100), secondSync = StatusUtil.ingestedUntil(200);

    Status order1 = combiner.typedReduce(key, Arrays.asList(newFile, firstSync, secondSync).iterator()), order2 = combiner.typedReduce(key,
        Arrays.asList(newFile, secondSync, firstSync).iterator());

    Assert.assertEquals(order1, order2);
  }

  @Test
  public void commutativeNewUpdatesSingleBuilder() {
    Status newFile = StatusUtil.fileCreated(100), firstSync = StatusUtil.ingestedUntil(builder, 100), secondSync = StatusUtil.ingestedUntil(builder, 200);

    Status order1 = combiner.typedReduce(key, Arrays.asList(newFile, firstSync, secondSync).iterator()), order2 = combiner.typedReduce(key,
        Arrays.asList(newFile, secondSync, firstSync).iterator());

    Assert.assertEquals(order1, order2);
  }

  @Test
  public void commutativeWithClose() {
    Status newFile = StatusUtil.fileCreated(100), closed = StatusUtil.fileClosed(), secondSync = StatusUtil.ingestedUntil(200);

    Status order1 = combiner.typedReduce(key, Arrays.asList(newFile, closed, secondSync).iterator()), order2 = combiner.typedReduce(key,
        Arrays.asList(newFile, secondSync, closed).iterator());

    Assert.assertEquals(order1, order2);
  }

  @Test
  public void commutativeWithCloseSingleBuilder() {
    Status newFile = StatusUtil.fileCreated(100), closed = StatusUtil.fileClosed(), secondSync = StatusUtil.ingestedUntil(builder, 200);

    Status order1 = combiner.typedReduce(key, Arrays.asList(newFile, closed, secondSync).iterator()), order2 = combiner.typedReduce(key,
        Arrays.asList(newFile, secondSync, closed).iterator());

    Assert.assertEquals(order1, order2);
  }

  @Test
  public void commutativeWithMultipleUpdates() {
    Status newFile = StatusUtil.fileCreated(100), update1 = StatusUtil.ingestedUntil(100), update2 = StatusUtil.ingestedUntil(200), repl1 = StatusUtil
        .replicated(50), repl2 = StatusUtil.replicated(150);

    Status order1 = combiner.typedReduce(key, Arrays.asList(newFile, update1, repl1, update2, repl2).iterator());

    // Got all replication updates before ingest updates
    Status permutation = combiner.typedReduce(key, Arrays.asList(newFile, repl1, update1, repl2, update2).iterator());

    Assert.assertEquals(order1, permutation);

    // All replications before updates
    permutation = combiner.typedReduce(key, Arrays.asList(newFile, repl1, repl2, update1, update2).iterator());

    Assert.assertEquals(order1, permutation);

    // All updates before replications
    permutation = combiner.typedReduce(key, Arrays.asList(newFile, update1, update2, repl1, repl2, update1, update2).iterator());

    Assert.assertEquals(order1, permutation);
  }

  @Test
  public void commutativeWithMultipleUpdatesSingleBuilder() {
    Status newFile = StatusUtil.fileCreated(100), update1 = StatusUtil.ingestedUntil(builder, 100), update2 = StatusUtil.ingestedUntil(builder, 200), repl1 = StatusUtil
        .replicated(builder, 50), repl2 = StatusUtil.replicated(builder, 150);

    Status order1 = combiner.typedReduce(key, Arrays.asList(newFile, update1, repl1, update2, repl2).iterator());

    // Got all replication updates before ingest updates
    Status permutation = combiner.typedReduce(key, Arrays.asList(newFile, repl1, update1, repl2, update2).iterator());

    Assert.assertEquals(order1, permutation);

    // All replications before updates
    permutation = combiner.typedReduce(key, Arrays.asList(newFile, repl1, repl2, update1, update2).iterator());

    Assert.assertEquals(order1, permutation);

    // All updates before replications
    permutation = combiner.typedReduce(key, Arrays.asList(newFile, update1, update2, repl1, repl2).iterator());

    Assert.assertEquals(order1, permutation);
  }

  @Test
  public void duplicateStatuses() {
    Status newFile = StatusUtil.fileCreated(100), update1 = StatusUtil.ingestedUntil(builder, 100), update2 = StatusUtil.ingestedUntil(builder, 200), repl1 = StatusUtil
        .replicated(builder, 50), repl2 = StatusUtil.replicated(builder, 150);

    Status order1 = combiner.typedReduce(key, Arrays.asList(newFile, update1, repl1, update2, repl2).iterator());

    // Repeat the same thing more than once
    Status permutation = combiner.typedReduce(key, Arrays.asList(newFile, repl1, update1, update1, repl2, update2, update2).iterator());

    Assert.assertEquals(order1, permutation);
  }

  @Test
  public void fileClosedTimePropagated() {
    Status stat1 = Status.newBuilder().setBegin(10).setEnd(20).setClosed(true).setInfiniteEnd(false).setCreatedTime(50).build();
    Status stat2 = Status.newBuilder().setBegin(10).setEnd(20).setClosed(true).setInfiniteEnd(false).build();

    Status combined = combiner.typedReduce(key, Arrays.asList(stat1, stat2).iterator());

    Assert.assertEquals(stat1, combined);
  }

  @Test
  public void fileClosedTimeChoosesEarliestIgnoringDefault() {
    Status stat1 = Status.newBuilder().setBegin(10).setEnd(20).setClosed(true).setInfiniteEnd(false).setCreatedTime(50).build();
    Status stat2 = Status.newBuilder().setBegin(10).setEnd(20).setClosed(true).setInfiniteEnd(false).setCreatedTime(100).build();

    Status combined = combiner.typedReduce(key, Arrays.asList(stat1, stat2).iterator());

    Assert.assertEquals(stat1, combined);

    Status stat3 = Status.newBuilder().setBegin(10).setEnd(20).setClosed(true).setInfiniteEnd(false).setCreatedTime(100).build();

    Status combined2 = combiner.typedReduce(key, Arrays.asList(combined, stat3).iterator());

    Assert.assertEquals(combined, combined2);
  }

  @Test
  public void testCombination() {
    List<Status> status = new ArrayList<>();
    long time = System.currentTimeMillis();

    status.add(StatusUtil.fileCreated(time));
    status.add(StatusUtil.openWithUnknownLength());
    status.add(StatusUtil.fileClosed());

    Status combined = combiner.typedReduce(new Key("row"), status.iterator());

    Assert.assertEquals(time, combined.getCreatedTime());
    Assert.assertTrue(combined.getInfiniteEnd());
    Assert.assertTrue(combined.getClosed());
  }
}
