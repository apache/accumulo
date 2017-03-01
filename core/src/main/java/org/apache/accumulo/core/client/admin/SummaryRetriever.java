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

package org.apache.accumulo.core.client.admin;

import java.util.Collection;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.client.summary.Summary;
import org.apache.hadoop.io.Text;

/**
 * This interface allows configuring where and which summary data to retrieve before retrieving it.
 *
 * @since 2.0.0
 */
public interface SummaryRetriever {

  /**
   * Summary data is only retrieved from data that has been written to files. Data recently written to Accumulo may be in memory and there will not show up in
   * summary data. Setting this option to true force tablets in the range to minor compact before summary data is retrieved. By default the table will not be
   * flushed before retrieving summary data.
   *
   * @return this
   */
  SummaryRetriever flush(boolean shouldFlush);

  /**
   * Allows optionally setting start row before retrieving data. The start row is not inclusive.
   */
  SummaryRetriever startRow(Text startRow);

  /**
   * Allows optionally setting start row before retrieving data. The start row is not inclusive.
   */
  SummaryRetriever startRow(CharSequence startRow);

  /**
   * Allows optionally setting end row before retrieving data. The end row is inclusive.
   */
  SummaryRetriever endRow(Text endRow);

  /**
   * Allows optionally setting end row before retrieving data. The end row is inclusive.
   */
  SummaryRetriever endRow(CharSequence endRow);

  /**
   * Gets summaries generated with a configuration that matches the given regex. For a given SummarizationConfiguration it is matched in exactly the following
   * way. This allows the regex to match on classname and options.
   *
   * <pre>
   * <code>
   *    boolean doesConfigurationMatch(SummarizerConfiguration conf, String regex) {
   *      // This is how conf is converted to a String in tablet servers for matching.
   *      // The options are sorted to make writing regular expressions easier.
   *      String confString = conf.getClassName()+" "+new TreeMap&lt;&gt;(conf.getOptions());
   *      return Pattern.compile(regex).matcher(confString).matches();
   *    }
   * </code>
   * </pre>
   *
   * <p>
   * Using this method to be more selective may pull less data in to the tablet servers summary cache.
   *
   */
  SummaryRetriever withMatchingConfiguration(String regex);

  /**
   * Allows specifying a set of summaries, generated using the specified configs, to retrieve. By default will retrieve all present.
   *
   * <p>
   * Using this method to be more selective may pull less data in to the tablet servers summary cache.
   */
  SummaryRetriever withConfiguration(SummarizerConfiguration... config);

  /**
   * Allows specifying a set of summaries, generated using the specified configs, to retrieve. By default will retrieve all present.
   *
   * <p>
   * Using this method to be more selective may pull less data in to the tablet servers summary cache.
   */
  SummaryRetriever withConfiguration(Collection<SummarizerConfiguration> configs);

  /**
   * @return a map of counter groups to counts
   */
  List<Summary> retrieve() throws AccumuloException, AccumuloSecurityException, TableNotFoundException;
}
