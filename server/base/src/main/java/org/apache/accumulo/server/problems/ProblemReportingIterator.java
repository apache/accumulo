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
package org.apache.accumulo.server.problems;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.InterruptibleIterator;
import org.apache.accumulo.server.ServerContext;

public class ProblemReportingIterator implements InterruptibleIterator {
  private final SortedKeyValueIterator<Key,Value> source;
  private boolean sawError = false;
  private final boolean continueOnError;
  private String resource;
  private TableId tableId;
  private final ServerContext context;

  public ProblemReportingIterator(ServerContext context, TableId tableId, String resource,
      boolean continueOnError, SortedKeyValueIterator<Key,Value> source) {
    this.context = context;
    this.tableId = tableId;
    this.resource = resource;
    this.continueOnError = continueOnError;
    this.source = source;
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    return new ProblemReportingIterator(context, tableId, resource, continueOnError,
        source.deepCopy(env));
  }

  @Override
  public Key getTopKey() {
    return source.getTopKey();
  }

  @Override
  public Value getTopValue() {
    return source.getTopValue();
  }

  @Override
  public boolean hasTop() {
    if (sawError) {
      return false;
    }
    return source.hasTop();
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void next() throws IOException {
    try {
      source.next();
    } catch (IOException ioe) {
      sawError = true;
      ProblemReports.getInstance(context)
          .report(new ProblemReport(tableId, ProblemType.FILE_READ, resource, ioe));
      if (!continueOnError) {
        throw ioe;
      }
    }
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    if (continueOnError && sawError) {
      return;
    }

    try {
      source.seek(range, columnFamilies, inclusive);
    } catch (IOException ioe) {
      sawError = true;
      ProblemReports.getInstance(context)
          .report(new ProblemReport(tableId, ProblemType.FILE_READ, resource, ioe));
      if (!continueOnError) {
        throw ioe;
      }
    }
  }

  public boolean sawError() {
    return sawError;
  }

  public String getResource() {
    return resource;
  }

  @Override
  public void setInterruptFlag(AtomicBoolean flag) {
    ((InterruptibleIterator) source).setInterruptFlag(flag);
  }
}
