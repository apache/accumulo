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
package org.apache.accumulo.core.summary;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.hadoop.io.WritableUtils;

public class SummaryWriter implements FileSKVWriter {

  static final String METASTORE_PREFIX = "accumulo.summaries";
  static final String METASTORE_INDEX = "accumulo.summaries.index";

  // echo "accumulo summarize" | sha1sum | head -c 8
  static long MAGIC = 0x15ea283ec03e4c49L;
  static byte VER = 1;

  private FileSKVWriter writer;
  private SummarySerializer.Builder[] summaryStores;

  private SummaryWriter(FileSKVWriter writer, SummarizerFactory factory,
      List<SummarizerConfiguration> configs, long maxSize) {
    this.writer = writer;
    int i = 0;
    summaryStores = new SummarySerializer.Builder[configs.size()];
    for (SummarizerConfiguration sconf : configs) {
      summaryStores[i++] = SummarySerializer.builder(sconf, factory, maxSize);
    }
  }

  @Override
  public boolean supportsLocalityGroups() {
    return true;
  }

  @Override
  public void startNewLocalityGroup(String name, Set<ByteSequence> columnFamilies)
      throws IOException {
    for (SummarySerializer.Builder ssb : summaryStores) {
      ssb.startNewLocalityGroup(name);
    }

    writer.startNewLocalityGroup(name, columnFamilies);
  }

  @Override
  public void startDefaultLocalityGroup() throws IOException {
    for (SummarySerializer.Builder ssb : summaryStores) {
      ssb.startDefaultLocalityGroup();
    }
    writer.startDefaultLocalityGroup();
  }

  @Override
  public void append(Key key, Value value) throws IOException {
    writer.append(key, value);
    for (SummarySerializer.Builder ssb : summaryStores) {
      ssb.put(key, value);
    }
  }

  @Override
  public DataOutputStream createMetaStore(String name) throws IOException {
    return writer.createMetaStore(name);
  }

  public void writeConfig(SummarizerConfiguration conf, DataOutputStream dos) throws IOException {
    // save class (and its config) used to generate summaries
    dos.writeUTF(conf.getClassName());
    dos.writeUTF(conf.getPropertyId());
    WritableUtils.writeVInt(dos, conf.getOptions().size());
    for (Entry<String,String> entry : conf.getOptions().entrySet()) {
      dos.writeUTF(entry.getKey());
      dos.writeUTF(entry.getValue());
    }
  }

  @Override
  public void close() throws IOException {

    DataOutputStream out = writer.createMetaStore(METASTORE_INDEX);
    out.writeLong(MAGIC);
    out.write(VER);
    WritableUtils.writeVInt(out, summaryStores.length);

    // Could possibly inline small summaries in the future. Breaking summaries into multiple block
    // is better for caching a subset of summaries. Also, keeping
    // the index small is good for the case where summaries that do not exist are requested. However
    // multiple blocks cause more random I/O in the case when its
    // not yet in the cache.

    for (int i = 0; i < summaryStores.length; i++) {
      writeConfig(summaryStores[i].getSummarizerConfiguration(), out);
      // write if summary is inlined in index... support for possible future optimizations.
      out.writeBoolean(false);
      // write pointer to block that will contain summary data
      WritableUtils.writeVInt(out, i);
      // write offset of summary data within block. This is not currently used, but it supports
      // storing multiple summaries in an external block in the
      // future without changing the code.
      WritableUtils.writeVInt(out, 0);
    }
    out.close();

    for (int i = 0; i < summaryStores.length; i++) {
      DataOutputStream summaryOut = writer.createMetaStore(METASTORE_PREFIX + "." + i);
      summaryStores[i].save(summaryOut);
      summaryOut.close();
    }

    writer.close();
  }

  @Override
  public long getLength() throws IOException {
    return writer.getLength();
  }

  public static FileSKVWriter wrap(FileSKVWriter writer, AccumuloConfiguration tableConfig,
      boolean useAccumuloStart) {
    List<SummarizerConfiguration> configs =
        SummarizerConfigurationUtil.getSummarizerConfigs(tableConfig);

    if (configs.isEmpty()) {
      return writer;
    }

    SummarizerFactory factory;
    if (useAccumuloStart) {
      factory = new SummarizerFactory(tableConfig);
    } else {
      factory = new SummarizerFactory();
    }

    long maxSize = tableConfig.getAsBytes(Property.TABLE_FILE_SUMMARY_MAX_SIZE);
    return new SummaryWriter(writer, factory, configs, maxSize);
  }
}
