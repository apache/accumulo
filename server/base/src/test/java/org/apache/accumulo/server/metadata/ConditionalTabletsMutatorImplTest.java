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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.StreamSupport;

import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.LockID;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

public class ConditionalTabletsMutatorImplTest {

  // a conditional tablet mutator that always returns a supplied status
  static class TestConditionalTabletsMutator extends ConditionalTabletsMutatorImpl {

    private final Map<KeyExtent,TabletMetadata> failedExtents;
    private final List<Function<Text,ConditionalWriter.Status>> statuses;

    private int attempt = 0;

    public TestConditionalTabletsMutator(ServerContext context,
        List<Function<Text,ConditionalWriter.Status>> statuses,
        Map<KeyExtent,TabletMetadata> failedExtents) {
      super(context);
      this.statuses = statuses;
      this.failedExtents = failedExtents;
    }

    protected Map<KeyExtent,TabletMetadata> readTablets(List<KeyExtent> extents) {
      return failedExtents;
    }

    protected ConditionalWriter createConditionalWriter(Ample.DataLevel dataLevel)
        throws TableNotFoundException {
      return new ConditionalWriter() {
        @Override
        public Iterator<Result> write(Iterator<ConditionalMutation> mutations) {
          Iterable<ConditionalMutation> iterable = () -> mutations;
          var localAttempt = attempt++;
          return StreamSupport.stream(iterable.spliterator(), false)
              .map(cm -> new Result(statuses.get(localAttempt).apply(new Text(cm.getRow())), cm,
                  "server"))
              .iterator();
        }

        @Override
        public Result write(ConditionalMutation mutation) {
          return write(List.of(mutation).iterator()).next();
        }

        @Override
        public void close() {

        }
      };
    }
  }

  @Test
  public void testRejectionHandler() {

    ServerContext context = EasyMock.createMock(ServerContext.class);
    EasyMock.expect(context.getZooKeeperRoot()).andReturn("/some/path").anyTimes();
    ServiceLock lock = EasyMock.createMock(ServiceLock.class);
    LockID lid = EasyMock.createMock(LockID.class);
    EasyMock.expect(lock.getLockID()).andReturn(lid).anyTimes();
    EasyMock.expect(lid.serialize("/some/path/")).andReturn("/some/path/1234").anyTimes();
    EasyMock.expect(context.getServiceLock()).andReturn(lock).anyTimes();
    EasyMock.replay(context, lock, lid);

    // this test checks the handling of conditional mutations that return a status of unknown and
    // rejected

    var ke1 = new KeyExtent(TableId.of("1"), null, null);

    TabletMetadata tm1 = EasyMock.createMock(TabletMetadata.class);
    EasyMock.expect(tm1.getDirName()).andReturn("dir1").anyTimes();
    EasyMock.replay(tm1);

    var ke2 = new KeyExtent(TableId.of("a"), null, null);

    TabletMetadata tm2 = EasyMock.createMock(TabletMetadata.class);
    EasyMock.expect(tm2.getDirName()).andReturn("dir2").anyTimes();
    EasyMock.replay(tm2);

    var ke3 = new KeyExtent(TableId.of("b"), null, null);

    TabletMetadata tm3 = EasyMock.createMock(TabletMetadata.class);
    EasyMock.expect(tm3.getDirName()).andReturn("dir4").anyTimes();
    EasyMock.replay(tm3);

    var ke4 = new KeyExtent(TableId.of("c"), null, null);

    TabletMetadata tm4 = EasyMock.createMock(TabletMetadata.class);
    EasyMock.expect(tm4.getDirName()).andReturn("dir5").anyTimes();
    EasyMock.replay(tm4);

    var failedExtents = Map.of(ke1, tm1, ke2, tm2, ke3, tm3, ke4, tm4);

    // expect retry on unknown
    var statuses1 = Map.of(ke1.toMetaRow(), ConditionalWriter.Status.UNKNOWN, ke2.toMetaRow(),
        ConditionalWriter.Status.UNKNOWN, ke3.toMetaRow(), ConditionalWriter.Status.REJECTED,
        ke4.toMetaRow(), ConditionalWriter.Status.ACCEPTED);

    // on the 2nd retry, return rejected
    var statuses2 = Map.of(ke1.toMetaRow(), ConditionalWriter.Status.REJECTED, ke2.toMetaRow(),
        ConditionalWriter.Status.REJECTED);

    try (var mutator = new TestConditionalTabletsMutator(context,
        List.of(statuses1::get, statuses2::get), failedExtents)) {

      mutator.mutateTablet(ke1).requireAbsentOperation().putDirName("dir1")
          .submit(tmeta -> tmeta.getDirName().equals("dir1"));

      mutator.mutateTablet(ke2).requireAbsentOperation().putDirName("dir3")
          .submit(tmeta -> tmeta.getDirName().equals("dir3"));

      mutator.mutateTablet(ke3).requireAbsentOperation().putDirName("dir4")
          .submit(tmeta -> tmeta.getDirName().equals("dir4"));

      mutator.mutateTablet(ke4).requireAbsentOperation().putDirName("dir5").submit(tmeta -> {
        throw new IllegalStateException();
      });

      Map<KeyExtent,Ample.ConditionalResult> results = mutator.process();

      assertEquals(Set.of(ke1, ke2, ke3, ke4), results.keySet());
      assertEquals(Ample.ConditionalResult.Status.ACCEPTED, results.get(ke1).getStatus());
      assertEquals(Ample.ConditionalResult.Status.REJECTED, results.get(ke2).getStatus());
      assertEquals(Ample.ConditionalResult.Status.ACCEPTED, results.get(ke3).getStatus());
      assertEquals(Ample.ConditionalResult.Status.ACCEPTED, results.get(ke4).getStatus());

      EasyMock.verify(context, lock, tm1, tm2, tm3, tm4, lid);

    }
  }
}
