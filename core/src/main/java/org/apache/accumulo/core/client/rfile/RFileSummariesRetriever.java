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

package org.apache.accumulo.core.client.rfile;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.function.Predicate;

import org.apache.accumulo.core.client.rfile.RFile.SummaryFSOptions;
import org.apache.accumulo.core.client.rfile.RFile.SummaryInputArguments;
import org.apache.accumulo.core.client.rfile.RFile.SummaryOptions;
import org.apache.accumulo.core.client.rfile.RFileScannerBuilder.InputArgs;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.client.summary.Summary;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.summary.Gatherer;
import org.apache.accumulo.core.summary.SummarizerFactory;
import org.apache.accumulo.core.summary.SummaryCollection;
import org.apache.accumulo.core.summary.SummaryReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;

class RFileSummariesRetriever implements SummaryInputArguments, SummaryFSOptions, SummaryOptions {

  private Predicate<SummarizerConfiguration> summarySelector = sc -> true;
  private Text startRow;
  private InputArgs in;
  private Text endRow;

  @Override
  public SummaryOptions selectSummaries(Predicate<SummarizerConfiguration> summarySelector) {
    Objects.requireNonNull(summarySelector);
    this.summarySelector = summarySelector;
    return this;
  }

  @Override
  public SummaryOptions startRow(CharSequence startRow) {
    return startRow(new Text(startRow.toString()));
  }

  @Override
  public SummaryOptions startRow(Text startRow) {
    Objects.requireNonNull(startRow);
    this.startRow = startRow;
    return this;
  }

  @Override
  public SummaryOptions endRow(CharSequence endRow) {
    return endRow(new Text(endRow.toString()));
  }

  @Override
  public SummaryOptions endRow(Text endRow) {
    Objects.requireNonNull(endRow);
    this.endRow = endRow;
    return this;
  }

  @Override
  public Collection<Summary> read() throws IOException {
    SummarizerFactory factory = new SummarizerFactory();
    AccumuloConfiguration acuconf = DefaultConfiguration.getInstance();
    Configuration conf = in.getFileSystem().getConf();

    RFileSource[] sources = in.getSources();
    try {
      SummaryCollection all = new SummaryCollection();
      for (RFileSource source : in.getSources()) {
        SummaryReader fileSummary = SummaryReader.load(conf, acuconf, source.getInputStream(), source.getLength(), summarySelector, factory);
        SummaryCollection sc = fileSummary.getSummaries(Collections.singletonList(new Gatherer.RowRange(startRow, endRow)));
        all.merge(sc, factory);
      }

      return all.getSummaries();
    } finally {
      for (RFileSource source : sources) {
        source.getInputStream().close();
      }
    }
  }

  @Override
  public SummaryOptions withFileSystem(FileSystem fs) {
    Objects.requireNonNull(fs);
    this.in.fs = fs;
    return this;
  }

  @Override
  public SummaryOptions from(RFileSource... inputs) {
    Objects.requireNonNull(inputs);
    in = new InputArgs(inputs);
    return this;
  }

  @Override
  public SummaryFSOptions from(String... files) {
    Objects.requireNonNull(files);
    in = new InputArgs(files);
    return this;
  }
}
