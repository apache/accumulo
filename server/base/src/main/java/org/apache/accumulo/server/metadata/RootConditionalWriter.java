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
package org.apache.accumulo.server.metadata;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.clientImpl.CompressedIterators;
import org.apache.accumulo.core.clientImpl.ConditionalWriterImpl;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.dataImpl.thrift.TCMResult;
import org.apache.accumulo.core.dataImpl.thrift.TConditionalMutation;
import org.apache.accumulo.core.iteratorsImpl.system.ColumnFamilySkippingIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.RootTabletMetadata;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.constraints.MetadataConstraints;
import org.apache.accumulo.server.data.ServerConditionalMutation;
import org.apache.accumulo.server.tablets.ConditionCheckerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A specialized conditional writer that writes metadata for the Accumulo root tablet which is
 * stored in zookeeper.
 */
public class RootConditionalWriter implements ConditionalWriter {

  private static final Logger log = LoggerFactory.getLogger(RootConditionalWriter.class);

  private final ServerContext context;

  RootConditionalWriter(ServerContext context) {
    this.context = context;
  }

  @Override
  public Iterator<Result> write(Iterator<ConditionalMutation> mutations) {
    List<Result> results = new ArrayList<>();
    while (mutations.hasNext()) {
      results.add(write(mutations.next()));
    }

    return results.iterator();
  }

  @Override
  public Result write(ConditionalMutation mutation) {

    MetadataConstraints metaConstraint = new MetadataConstraints();
    List<Short> violations =
        metaConstraint.check(new RootTabletMutatorImpl.RootEnv(context), mutation);

    if (violations != null && !violations.isEmpty()) {
      return new Result(Status.VIOLATED, mutation, "ZK");
    }

    CompressedIterators compressedIters = new CompressedIterators();
    TConditionalMutation tcm =
        ConditionalWriterImpl.convertConditionalMutation(compressedIters, mutation, 1);
    ConditionCheckerContext checkerContext = new ConditionCheckerContext(context, compressedIters,
        context.getTableConfiguration(AccumuloTable.ROOT.tableId()));

    ServerConditionalMutation scm = new ServerConditionalMutation(tcm);

    String zpath = context.getZooKeeperRoot() + RootTable.ZROOT_TABLET;

    context.getZooCache().clear(zpath);

    List<ServerConditionalMutation> okMutations = new ArrayList<>();
    List<TCMResult> results = new ArrayList<>();

    try {
      context.getZooReaderWriter().mutateExisting(zpath, currVal -> {
        String currJson = new String(currVal, UTF_8);

        var rtm = new RootTabletMetadata(currJson);

        var iter = new ColumnFamilySkippingIterator(new SortedMapIterator(rtm.toKeyValues()));

        // This could be called multiple times so clear before calling
        okMutations.clear();
        results.clear();
        var checker = checkerContext.newChecker(List.of(scm), okMutations, results);
        try {
          checker.check(iter);
          if (getResult(okMutations, results, mutation).getStatus() == Status.ACCEPTED) {
            rtm.update(mutation);
            String newJson = rtm.toJson();
            log.debug("mutation: from:[{}] to: [{}]", currJson, newJson);
            return newJson.getBytes(UTF_8);
          } else {
            // conditions failed so make no updates
            return null;
          }

        } catch (IOException e) {
          throw new UncheckedIOException(e);
        } catch (AccumuloException e) {
          throw new RuntimeException(e);
        } catch (AccumuloSecurityException e) {
          throw new RuntimeException(e);
        }
      });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    // TODO this is racy...
    context.getZooCache().clear(zpath);

    return getResult(okMutations, results, mutation);
  }

  private static Result getResult(List<ServerConditionalMutation> okMutations,
      List<TCMResult> results, ConditionalMutation mutation) {
    if (okMutations.size() == 1 && results.isEmpty()) {
      return new Result(Status.ACCEPTED, mutation, "ZK");
    } else if (okMutations.isEmpty() && results.size() == 1) {
      var tresult = results.get(0);
      return new Result(ConditionalWriterImpl.fromThrift(tresult.getStatus()), mutation, "ZK");
    } else {
      throw new AssertionError("Unexpected case");
    }
  }

  @Override
  public void close() {

  }
}
