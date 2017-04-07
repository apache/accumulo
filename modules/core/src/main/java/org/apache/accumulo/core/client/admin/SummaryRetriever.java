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
   * Forces a flush of data in memory to files before summary data is retrieved. Data recently written to Accumulo may be in memory. Summary data is only
   * retrieved from files. Therefore recently written data may not be represented in summaries, unless this options is set to true. This is optional and
   * defaults to false.
   *
   * @return this
   */
  SummaryRetriever flush(boolean shouldFlush);

  /**
   * The start row is not inclusive. Calling this method is optional.
   */
  SummaryRetriever startRow(Text startRow);

  /**
   * The start row is not inclusive. Calling this method is optional.
   */
  SummaryRetriever startRow(CharSequence startRow);

  /**
   * The end row is inclusive. Calling this method is optional.
   */
  SummaryRetriever endRow(Text endRow);

  /**
   * The end row is inclusive. Calling this method is optional.
   */
  SummaryRetriever endRow(CharSequence endRow);

  /**
   * Filters which summary data is retrieved. By default all summary data present is retrieved. If only a subset of summary data is needed, then its best to be
   * selective in order to avoid polluting summary data cache.
   *
   * <p>
   * Each set of summary data is generated using a specific {@link SummarizerConfiguration}. The methods {@link #withConfiguration(Collection)} and
   * {@link #withConfiguration(SummarizerConfiguration...)} allow selecting sets of summary data based on exact {@link SummarizerConfiguration} matches. This
   * method enables less exact matching using regular expressions.
   *
   * <p>
   * The regular expression passed to this method is used in the following way on the server side to match {@link SummarizerConfiguration} object. When a
   * {@link SummarizerConfiguration} matches, the summary data generated using that configuration is returned.
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
