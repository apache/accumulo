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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.SortedMap;
import java.util.function.BiPredicate;
import java.util.function.Consumer;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.client.admin.TabletInformation;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.ConditionalTabletsMutator;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataTime;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata.TableOptions;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.MoreCollectors;

public class TestAmple {

  private static final ConditionalWriterInterceptor EMPTY_INTERCEPTOR =
      new ConditionalWriterInterceptor() {};

  private static final BiPredicate<Key,Value> INCLUDE_ALL_COLUMNS = (k, v) -> true;

  public static BiPredicate<Key,Value> matches(ColumnFQ column) {
    return (k, v) -> column.equals(new ColumnFQ(k));
  }

  public static BiPredicate<Key,Value> not(ColumnFQ column) {
    return matches(column).negate();
  }

  public static Ample create(ServerContext context, Map<DataLevel,String> tables,
      Supplier<ConditionalWriterInterceptor> cwInterceptor) {
    return new TestServerAmpleImpl(context, tables, cwInterceptor);
  }

  public static Ample create(ServerContext context, Map<DataLevel,String> tables) {
    return create(context, tables, () -> EMPTY_INTERCEPTOR);
  }

  public static class TestServerAmpleImpl extends ServerAmpleImpl {

    private final Map<DataLevel,String> tables;
    private final Supplier<ConditionalWriterInterceptor> cwInterceptor;
    private final Supplier<ServerContext> testContext =
        Suppliers.memoize(() -> testAmpleServerContext(super.getContext(), this));

    private TestServerAmpleImpl(ServerContext context, Map<DataLevel,String> tables,
        Supplier<ConditionalWriterInterceptor> cwInterceptor) {
      super(context, tables::get);
      this.tables = Map.copyOf(tables);
      this.cwInterceptor = Objects.requireNonNull(cwInterceptor);
      Preconditions.checkArgument(!tables.containsKey(DataLevel.ROOT));
      Preconditions.checkArgument(
          tables.containsKey(DataLevel.USER) || tables.containsKey(DataLevel.METADATA));
    }

    @Override
    public TabletsMutator mutateTablets() {
      return new TabletsMutatorImpl(getContext(), tables::get);
    }

    @Override
    public ConditionalTabletsMutator conditionallyMutateTablets() {
      return conditionallyMutateTablets(cwInterceptor.get());
    }

    @Override
    public AsyncConditionalTabletsMutator
        conditionallyMutateTablets(Consumer<ConditionalResult> resultsConsumer) {
      return new AsyncConditionalTabletsMutatorImpl(getContext(), getTableMapper(),
          resultsConsumer);
    }

    @Override
    protected TableOptions newBuilder() {
      return TabletsMetadata.builder(getContext(), getTableMapper());
    }

    /**
     * Create default metadata for a Table
     *
     * TODO: Add a way to pass in options for config
     *
     * @param tableId The id of the table to create metadata for
     */
    public void createMetadata(TableId tableId) {
      try (var tabletsMutator = mutateTablets()) {
        var extent = new KeyExtent(tableId, null, null);
        var tabletMutator = tabletsMutator.mutateTablet(extent);
        String dirName = ServerColumnFamily.DEFAULT_TABLET_DIR_NAME;
        tabletMutator.putPrevEndRow(extent.prevEndRow());
        tabletMutator.putDirName(dirName);
        tabletMutator.putTime(new MetadataTime(0, TimeType.MILLIS));
        tabletMutator.putTabletAvailability(TabletAvailability.HOSTED);
        tabletMutator.mutate();
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }

    /**
     * Create metadata for a table by copying existing metadata for the table from the metadata
     * table in an existing Accumulo instance
     *
     * TODO: Add config parents (such as a way to include/exclude what is copied, etc)
     *
     * @param client The client to scan the existing accumulo metadata table
     * @param tableId The id of the table to create metadata for
     * @throws Exception thrown for any error on metadata creation
     */
    public void createMetadataFromExisting(AccumuloClient client, TableId tableId)
        throws Exception {
      createMetadataFromExisting(client, tableId, INCLUDE_ALL_COLUMNS);
    }

    public void createMetadataFromExisting(AccumuloClient client, TableId tableId,
        BiPredicate<Key,Value> includeColumn) throws Exception {
      try (Scanner scanner =
          client.createScanner(AccumuloTable.METADATA.tableName(), Authorizations.EMPTY)) {
        scanner.setRange(TabletsSection.getRange(tableId));
        IteratorSetting iterSetting = new IteratorSetting(100, WholeRowIterator.class);
        scanner.addScanIterator(iterSetting);

        try (BatchWriter bw =
            client.createBatchWriter(getMetadataTableName(DataLevel.of(tableId)))) {
          for (Entry<Key,Value> entry : scanner) {
            final SortedMap<Key,Value> decodedRow =
                WholeRowIterator.decodeRow(entry.getKey(), entry.getValue());
            Text row = decodedRow.firstKey().getRow();
            Mutation m = new Mutation(row);

            decodedRow.entrySet().stream().filter(e -> includeColumn.test(e.getKey(), e.getValue()))
                .forEach(e -> m.put(e.getKey().getColumnFamily(), e.getKey().getColumnQualifier(),
                    e.getKey().getColumnVisibilityParsed(), e.getKey().getTimestamp(),
                    e.getValue()));
            bw.addMutation(m);
          }
        }
      }
    }

    private ConditionalTabletsMutator
        conditionallyMutateTablets(ConditionalWriterInterceptor interceptor) {
      Objects.requireNonNull(interceptor);

      return new ConditionalTabletsMutatorImpl(getContext(), tables::get) {

        @Override
        protected ConditionalWriter createConditionalWriter(Ample.DataLevel dataLevel)
            throws TableNotFoundException {
          if (dataLevel == Ample.DataLevel.ROOT) {
            return super.createConditionalWriter(dataLevel);
          } else {
            return new ConditionalWriterDelegator(
                getContext().createConditionalWriter(getTableMapper().apply(dataLevel)),
                interceptor);
          }
        }
      };
    }

    @Override
    ServerContext getContext() {
      return testContext.get();
    }

  }

  public static ServerContext testAmpleServerContext(ServerContext context,
      TestAmple.TestServerAmpleImpl ample) {
    SiteConfiguration siteConfig;
    try {
      Map<String,String> propsMap = new HashMap<>();
      context.getSiteConfiguration().getProperties(propsMap, x -> true);
      siteConfig = SiteConfiguration.empty().withOverrides(propsMap).build();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return new ServerContext(siteConfig) {
      @Override
      public Ample getAmple() {
        return ample;
      }

      @Override
      public ServiceLock getServiceLock() {
        return context.getServiceLock();
      }

      @Override
      public void setServiceLock(ServiceLock lock) {
        context.setServiceLock(lock);
      }
    };
  }

  public static void createMetadataTable(ClientContext client, String table) throws Exception {
    final var metadataTableProps =
        client.tableOperations().getTableProperties(AccumuloTable.METADATA.tableName());

    TabletAvailability availability;
    try (var tabletStream = client.tableOperations()
        .getTabletInformation(AccumuloTable.METADATA.tableName(), new Range())) {
      availability = tabletStream.map(TabletInformation::getTabletAvailability).distinct()
          .collect(MoreCollectors.onlyElement());
    }

    var newTableConf = new NewTableConfiguration().withInitialTabletAvailability(availability)
        .withoutDefaultIterators().setProperties(metadataTableProps);
    client.tableOperations().create(table, newTableConf);
  }
}
