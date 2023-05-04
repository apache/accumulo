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
import org.apache.accumulo.core.client.ConditionalWriter.Status;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

public class ConditionalTabletsMutatorImplTest {

  // a conditional tablet mutator that always returns a supplied status
  static class TestConditionalTabletsMutator extends ConditionalTabletsMutatorImpl {

    private final Map<KeyExtent,TabletMetadata> failedExtents;
    private final Function<Text,Status> statuses;

    public TestConditionalTabletsMutator(Function<Text,Status> statuses,
        Map<KeyExtent,TabletMetadata> failedExtents) {
      super(null);
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
          return StreamSupport.stream(iterable.spliterator(), false)
              .map(cm -> new Result(statuses.apply(new Text(cm.getRow())), cm, "server"))
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
  public void testUnknownValidation() {

    // this test checks the handling of conditional mutations that return a status of unknown

    var ke1 = new KeyExtent(TableId.of("1"), null, null);

    TabletMetadata tm1 = EasyMock.createMock(TabletMetadata.class);
    EasyMock.expect(tm1.getDirName()).andReturn("dir1").anyTimes();
    EasyMock.replay(tm1);

    var ke2 = new KeyExtent(TableId.of("a"), null, null);

    TabletMetadata tm2 = EasyMock.createMock(TabletMetadata.class);
    EasyMock.expect(tm2.getDirName()).andReturn("dir2").anyTimes();
    EasyMock.replay(tm2);

    var ke3 = new KeyExtent(TableId.of("b"), null, null);
    var ke4 = new KeyExtent(TableId.of("c"), null, null);

    var failedExtents = Map.of(ke1, tm1, ke2, tm2);
    var statuses = Map.of(ke1.toMetaRow(), Status.UNKNOWN, ke2.toMetaRow(), Status.UNKNOWN,
        ke3.toMetaRow(), Status.REJECTED, ke4.toMetaRow(), Status.ACCEPTED);

    try (var mutator = new TestConditionalTabletsMutator(statuses::get, failedExtents)) {
      // passed in unknown handler should determine the mutations status should be accepted
      mutator.mutateTablet(ke1).requireAbsentOperation().putDirName("dir1")
          .submit(tmeta -> tmeta.getDirName().equals("dir1"));

      // passed in unknown handler should determine the mutations status should continue to be
      // UNKNOWN
      mutator.mutateTablet(ke2).requireAbsentOperation().putDirName("dir3")
          .submit(tmeta -> tmeta.getDirName().equals("dir3"));

      // ensure the unknown handler is only called when the status is unknown, this mutation will
      // have a status of REJECTED
      mutator.mutateTablet(ke3).requireAbsentOperation().putDirName("dir3").submit(tmeta -> {
        throw new IllegalStateException();
      });

      // ensure the unknown handler is only called when the status is unknown, this mutations will
      // have a status of ACCEPTED
      mutator.mutateTablet(ke4).requireAbsentOperation().putDirName("dir3").submit(tmeta -> {
        throw new IllegalStateException();
      });

      Map<KeyExtent,Ample.ConditionalResult> results = mutator.process();

      assertEquals(Set.of(ke1, ke2, ke3, ke4), results.keySet());
      assertEquals(Status.ACCEPTED, results.get(ke1).getStatus());
      assertEquals(Status.UNKNOWN, results.get(ke2).getStatus());
      assertEquals(Status.REJECTED, results.get(ke3).getStatus());
      assertEquals(Status.ACCEPTED, results.get(ke4).getStatus());
    }
  }
}
