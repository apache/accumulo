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
import static org.apache.accumulo.core.metadata.RootTable.ZROOT_TABLET_REFRESHES;
import static org.apache.accumulo.core.util.LazySingletons.GSON;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.RefreshSection;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class RefreshesImpl implements Ample.Refreshes {

  private static final Logger log = LoggerFactory.getLogger(RefreshesImpl.class);

  private final Ample.DataLevel dataLevel;
  private final ServerContext context;

  public static String getInitialJson() {
    return toJson(Map.of());
  }

  // the expected version of serialized data
  private static final int CURRENT_VERSION = 1;

  // This class is used to serialize and deserialize root tablet metadata using GSon. Any changes to
  // this class must consider persisted data.
  private static class Data {

    final int version;

    final Map<String,String> entries;

    public Data(int version, Map<String,String> entries) {
      this.version = version;
      this.entries = entries;
    }
  }

  private static String toJson(Map<String,String> entries) {
    var data = new Data(CURRENT_VERSION, Objects.requireNonNull(entries));
    return GSON.get().toJson(data, Data.class);
  }

  private Map<String,String> fromJson(String json) {
    Data data = GSON.get().fromJson(json, Data.class);
    Preconditions.checkArgument(data.version == CURRENT_VERSION, "Expected version %s saw %s",
        CURRENT_VERSION, data.version);
    Objects.requireNonNull(data.entries);
    return data.entries;
  }

  public RefreshesImpl(ServerContext context, Ample.DataLevel dataLevel) {
    this.context = context;
    this.dataLevel = dataLevel;
  }

  private void mutateRootRefreshes(Consumer<Map<String,String>> mutator) {
    String zpath = context.getZooKeeperRoot() + ZROOT_TABLET_REFRESHES;
    try {
      context.getZooReaderWriter().mutateExisting(zpath, currVal -> {
        String currJson = new String(currVal, UTF_8);
        log.debug("Root refreshes before change : {}", currJson);
        Map<String,String> entries = fromJson(currJson);
        mutator.accept(entries);
        String newJson = toJson(entries);
        log.debug("Root refreshes after change  : {}", newJson);
        if (newJson.length() > 262_144) {
          log.warn("Root refreshes stored in ZK at {} are getting large ({} bytes)."
              + " Large nodes may cause problems for Zookeeper!", zpath, newJson.length());
        }
        return newJson.getBytes(UTF_8);
      });
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private Text createRow(RefreshEntry entry) {
    return new Text(RefreshSection.getRowPrefix() + entry.getEcid().canonical());
  }

  private Mutation createAddMutation(RefreshEntry entry) {
    Mutation m = new Mutation(createRow(entry));

    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos)) {
      dos.writeInt(1);
      entry.getExtent().writeTo(dos);
      dos.writeUTF(entry.getTserver().getHostPortSession());
      dos.close();
      m.at().family("").qualifier("").put(baos.toByteArray());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    return m;
  }

  private Mutation createDeleteMutation(RefreshEntry entry) {
    Mutation m = new Mutation(createRow(entry));
    m.putDelete("", "");
    return m;
  }

  private void requireRootTablets(Collection<RefreshEntry> entries) {
    if (!entries.stream().allMatch(e -> e.getExtent().isRootTablet())) {
      var nonRootTablets = entries.stream().map(RefreshEntry::getExtent)
          .filter(e -> !e.isRootTablet()).collect(Collectors.toSet());
      throw new IllegalArgumentException("Expected only root tablet but saw " + nonRootTablets);
    }
  }

  private RefreshEntry decode(Map.Entry<Key,Value> entry) {
    String row = entry.getKey().getRowData().toString();
    Preconditions.checkArgument(row.startsWith(RefreshSection.getRowPrefix()),
        "Row %s did not start with %s", row, RefreshSection.getRowPrefix());
    Preconditions.checkArgument(entry.getKey().getColumnFamilyData().length() == 0,
        "Expected empty family but saw %s", entry.getKey().getColumnFamilyData());
    Preconditions.checkArgument(entry.getKey().getColumnQualifierData().length() == 0,
        "Expected empty qualifier but saw %s", entry.getKey().getColumnQualifierData());

    try (ByteArrayInputStream bais = new ByteArrayInputStream(entry.getValue().get());
        DataInputStream dis = new DataInputStream(bais)) {
      var version = dis.readInt();
      Preconditions.checkArgument(version == CURRENT_VERSION, "Expected version %s saw %s",
          CURRENT_VERSION, version);
      var extent = KeyExtent.readFrom(dis);
      var tserver = new TServerInstance(dis.readUTF());
      return new RefreshEntry(
          ExternalCompactionId.of(row.substring(RefreshSection.getRowPrefix().length())), extent,
          tserver);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void add(Collection<RefreshEntry> entries) {
    Objects.requireNonNull(entries);
    if (dataLevel == Ample.DataLevel.ROOT) {
      // expect all of these to be the root tablet, verifying because its not stored
      requireRootTablets(entries);
      Consumer<Map<String,String>> mutator = map -> entries.forEach(refreshEntry -> map
          .put(refreshEntry.getEcid().canonical(), refreshEntry.getTserver().getHostPortSession()));
      mutateRootRefreshes(mutator);
    } else {
      try (BatchWriter writer = context.createBatchWriter(dataLevel.metaTable())) {
        for (RefreshEntry entry : entries) {
          writer.addMutation(createAddMutation(entry));
        }
      } catch (MutationsRejectedException | TableNotFoundException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  @Override
  public void delete(Collection<RefreshEntry> entries) {
    Objects.requireNonNull(entries);
    if (dataLevel == Ample.DataLevel.ROOT) {
      // expect all of these to be the root tablet, verifying because its not stored
      requireRootTablets(entries);
      Consumer<Map<String,String>> mutator =
          map -> entries.forEach(refreshEntry -> map.remove(refreshEntry.getEcid().canonical()));
      mutateRootRefreshes(mutator);
    } else {
      try (BatchWriter writer = context.createBatchWriter(dataLevel.metaTable())) {
        for (RefreshEntry entry : entries) {
          writer.addMutation(createDeleteMutation(entry));
        }
      } catch (MutationsRejectedException | TableNotFoundException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  @Override
  public Stream<RefreshEntry> stream() {
    if (dataLevel == Ample.DataLevel.ROOT) {
      var zooReader = context.getZooReader();
      byte[] jsonBytes;
      try {
        jsonBytes =
            zooReader.getData(context.getZooKeeperRoot() + RootTable.ZROOT_TABLET_REFRESHES);
        return fromJson(new String(jsonBytes, UTF_8)).entrySet().stream()
            .map(e -> new RefreshEntry(ExternalCompactionId.of(e.getKey()), RootTable.EXTENT,
                new TServerInstance(e.getValue())));
      } catch (KeeperException | InterruptedException e) {
        throw new IllegalStateException(e);
      }
    } else {
      Range range = RefreshSection.getRange();

      Scanner scanner;
      try {
        scanner = context.createScanner(dataLevel.metaTable(), Authorizations.EMPTY);
      } catch (TableNotFoundException e) {
        throw new IllegalStateException(e);
      }
      scanner.setRange(range);
      return scanner.stream().onClose(scanner::close).map(this::decode);
    }
  }
}
