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
package org.apache.accumulo.core.clientImpl;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

public class OfflineScanner extends ScannerOptions implements Scanner {

  private int batchSize;
  private Range range;

  private ClientContext context;
  private Authorizations authorizations;
  private Text tableId;

  public OfflineScanner(ClientContext context, TableId tableId, Authorizations authorizations) {
    checkArgument(context != null, "context is null");
    checkArgument(tableId != null, "tableId is null");
    checkArgument(authorizations != null, "authorizations is null");
    this.context = context;
    this.tableId = new Text(tableId.canonical());
    this.range = new Range((Key) null, (Key) null);
    this.authorizations = authorizations;
    this.batchSize = Constants.SCAN_BATCH_SIZE;
  }

  @Override
  public void setRange(Range range) {
    this.range = range;
  }

  @Override
  public Range getRange() {
    return range;
  }

  @Override
  public void setBatchSize(int size) {
    this.batchSize = size;
  }

  @Override
  public int getBatchSize() {
    return batchSize;
  }

  @Override
  public void enableIsolation() {

  }

  @Override
  public void disableIsolation() {

  }

  @Override
  public Iterator<Entry<Key,Value>> iterator() {
    return new OfflineIterator(this, context, authorizations, tableId, range);
  }

  @Override
  public Authorizations getAuthorizations() {
    return authorizations;
  }

  @Override
  public long getReadaheadThreshold() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setReadaheadThreshold(long batches) {
    throw new UnsupportedOperationException();
  }

}
