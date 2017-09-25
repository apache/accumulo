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
package org.apache.accumulo.server.util;

import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.IteratorSetting.Column;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.impl.Writer;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.ReplicationSection;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.tabletserver.thrift.ConstraintViolationException;
import org.apache.accumulo.server.replication.StatusCombiner;
import org.apache.accumulo.server.replication.StatusFormatter;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * provides a reference to the replication table for updates by tablet servers
 */
public class ReplicationTableUtil {

  private static Map<Credentials,Writer> writers = new HashMap<>();
  private static final Logger log = LoggerFactory.getLogger(ReplicationTableUtil.class);

  public static final String COMBINER_NAME = "replcombiner";
  public static final String STATUS_FORMATTER_CLASS_NAME = StatusFormatter.class.getName();

  private ReplicationTableUtil() {}

  /**
   * For testing purposes only -- should not be called by server code
   * <p>
   * Allows mocking of a Writer for testing
   *
   * @param creds
   *          Credentials
   * @param writer
   *          A Writer to use for the given credentials
   */
  synchronized static void addWriter(Credentials creds, Writer writer) {
    writers.put(creds, writer);
  }

  synchronized static Writer getWriter(ClientContext context) {
    Writer replicationTable = writers.get(context.getCredentials());
    if (replicationTable == null) {
      Connector conn;
      try {
        conn = context.getConnector();
      } catch (AccumuloException | AccumuloSecurityException e) {
        throw new RuntimeException(e);
      }

      configureMetadataTable(conn, MetadataTable.NAME);

      replicationTable = new Writer(context, MetadataTable.ID);
      writers.put(context.getCredentials(), replicationTable);
    }
    return replicationTable;
  }

  public synchronized static void configureMetadataTable(Connector conn, String tableName) {
    TableOperations tops = conn.tableOperations();
    Map<String,EnumSet<IteratorScope>> iterators = null;
    try {
      iterators = tops.listIterators(tableName);
    } catch (AccumuloSecurityException | AccumuloException | TableNotFoundException e) {
      throw new RuntimeException(e);
    }

    if (!iterators.containsKey(COMBINER_NAME)) {
      // Set our combiner and combine all columns
      // Need to set the combiner beneath versioning since we don't want to turn it off
      IteratorSetting setting = new IteratorSetting(9, COMBINER_NAME, StatusCombiner.class);
      Combiner.setColumns(setting, Collections.singletonList(new Column(MetadataSchema.ReplicationSection.COLF)));
      try {
        tops.attachIterator(tableName, setting);
      } catch (AccumuloSecurityException | AccumuloException | TableNotFoundException e) {
        throw new RuntimeException(e);
      }
    }

    // Make sure the StatusFormatter is set on the metadata table
    Iterable<Entry<String,String>> properties;
    try {
      properties = tops.getProperties(tableName);
    } catch (AccumuloException | TableNotFoundException e) {
      throw new RuntimeException(e);
    }

    for (Entry<String,String> property : properties) {
      if (Property.TABLE_FORMATTER_CLASS.getKey().equals(property.getKey())) {
        if (!STATUS_FORMATTER_CLASS_NAME.equals(property.getValue())) {
          log.info("Setting formatter for {} from {} to {}", tableName, property.getValue(), STATUS_FORMATTER_CLASS_NAME);
          try {
            tops.setProperty(tableName, Property.TABLE_FORMATTER_CLASS.getKey(), STATUS_FORMATTER_CLASS_NAME);
          } catch (AccumuloException | AccumuloSecurityException e) {
            throw new RuntimeException(e);
          }
        }

        // Don't need to keep iterating over the properties after we found the one we were looking for
        return;
      }
    }

    // Set the formatter on the table because it wasn't already there
    try {
      tops.setProperty(tableName, Property.TABLE_FORMATTER_CLASS.getKey(), STATUS_FORMATTER_CLASS_NAME);
    } catch (AccumuloException | AccumuloSecurityException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Write the given Mutation to the replication table.
   */
  static void update(ClientContext context, Mutation m, KeyExtent extent) {
    Writer t = getWriter(context);
    while (true) {
      try {
        t.update(m);
        return;
      } catch (AccumuloException e) {
        log.error(e.toString(), e);
      } catch (AccumuloSecurityException e) {
        log.error(e.toString(), e);
      } catch (ConstraintViolationException e) {
        log.error(e.toString(), e);
      } catch (TableNotFoundException e) {
        log.error(e.toString(), e);
      }
      sleepUninterruptibly(1, TimeUnit.SECONDS);
    }
  }

  /**
   * Write replication ingest entries for each provided file with the given {@link Status}.
   */
  public static void updateFiles(ClientContext context, KeyExtent extent, String file, Status stat) {
    if (log.isDebugEnabled()) {
      log.debug("Updating replication status for {} with {} using {}", extent, file, ProtobufUtil.toString(stat));
    }
    // TODO could use batch writer, would need to handle failure and retry like update does - ACCUMULO-1294

    Value v = ProtobufUtil.toValue(stat);
    update(context, createUpdateMutation(new Path(file), v, extent), extent);
  }

  static Mutation createUpdateMutation(Path file, Value v, KeyExtent extent) {
    // Need to normalize the file path so we can assuredly find it again later
    return createUpdateMutation(new Text(ReplicationSection.getRowPrefix() + file.toString()), v, extent);
  }

  private static Mutation createUpdateMutation(Text row, Value v, KeyExtent extent) {
    Mutation m = new Mutation(row);
    m.put(MetadataSchema.ReplicationSection.COLF, new Text(extent.getTableId().getUtf8()), v);
    return m;
  }
}
