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

import java.text.DateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.ReplicationSection;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.ReplicationSchema.OrderSection;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.replication.ReplicationSchema.WorkSection;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.format.DefaultFormatter;
import org.apache.accumulo.core.util.format.Formatter;
import org.apache.accumulo.core.util.format.FormatterConfig;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Parse and print the serialized protocol buffers used to track replication data
 */
public class StatusFormatter implements Formatter {
  private static final Logger log = LoggerFactory.getLogger(StatusFormatter.class);

  private static final Set<Text> REPLICATION_COLFAMS = Collections.unmodifiableSet(Sets.newHashSet(ReplicationSection.COLF, StatusSection.NAME,
      WorkSection.NAME, OrderSection.NAME));

  private Iterator<Entry<Key,Value>> iterator;
  private FormatterConfig config;

  /* so a new date object doesn't get created for every record in the scan result */
  private static ThreadLocal<Date> tmpDate = new ThreadLocal<Date>() {
    @Override
    protected Date initialValue() {
      return new Date();
    }
  };

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public String next() {
    Entry<Key,Value> entry = iterator.next();
    DateFormat timestampFormat = config.willPrintTimestamps() ? config.getDateFormatSupplier().get() : null;

    // If we expected this to be a protobuf, try to parse it, adding a message when it fails to parse
    if (REPLICATION_COLFAMS.contains(entry.getKey().getColumnFamily())) {
      Status status;
      try {
        status = Status.parseFrom(entry.getValue().get());
      } catch (InvalidProtocolBufferException e) {
        log.trace("Could not deserialize protocol buffer for {}", entry.getKey(), e);
        status = null;
      }

      return formatEntry(entry.getKey(), status, timestampFormat);
    } else {
      // Otherwise, we're set on a table that contains other data too (e.g. accumulo.metadata)
      // Just do the normal thing
      return DefaultFormatter.formatEntry(entry, timestampFormat);
    }
  }

  public String formatEntry(Key key, Status status, DateFormat timestampFormat) {
    StringBuilder sb = new StringBuilder();
    Text buffer = new Text();

    // append row
    key.getRow(buffer);
    appendText(sb, buffer).append(" ");

    // append column family
    key.getColumnFamily(buffer);
    appendText(sb, buffer).append(":");

    // append column qualifier
    key.getColumnQualifier(buffer);
    appendText(sb, buffer).append(" ");

    // append visibility expression
    key.getColumnVisibility(buffer);
    sb.append(new ColumnVisibility(buffer));

    // append timestamp
    if (timestampFormat != null) {
      tmpDate.get().setTime(key.getTimestamp());
      sb.append(" ").append(timestampFormat.format(tmpDate.get()));
    }

    sb.append("\t");
    // append value
    if (status != null) {
      sb.append(ProtobufUtil.toString(status));
    } else {
      sb.append("Could not deserialize Status protocol buffer");
    }

    return sb.toString();
  }

  protected StringBuilder appendText(StringBuilder sb, Text t) {
    return appendBytes(sb, t.getBytes(), 0, t.getLength());
  }

  protected StringBuilder appendBytes(StringBuilder sb, byte ba[], int offset, int len) {
    for (int i = 0; i < len; i++) {
      int c = 0xff & ba[offset + i];
      if (c == '\\')
        sb.append("\\\\");
      else if (c >= 32 && c <= 126)
        sb.append((char) c);
      else
        sb.append("\\x").append(String.format("%02X", c));
    }
    return sb;
  }

  @Override
  public void remove() {
    iterator.remove();
  }

  @Override
  public void initialize(Iterable<Entry<Key,Value>> scanner, FormatterConfig config) {
    this.iterator = scanner.iterator();
    this.config = new FormatterConfig(config);
  }

}
