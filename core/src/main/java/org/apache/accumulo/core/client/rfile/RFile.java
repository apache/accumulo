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
import java.io.OutputStream;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;

/**
 * RFile is Accumulo's internal storage format for Key Value pairs. This class is a Factory that
 * enables creating a {@link Scanner} for reading and a {@link RFileWriter} for writing Rfiles.
 *
 * <p>
 * The {@link Scanner} created by this class makes it easy to experiment with real data from a live
 * system on a developers workstation. Also the {@link Scanner} can be used to write tools to
 * analyze Accumulo's raw data.
 *
 * @since 1.8.0
 */
public class RFile {

  /**
   * This is an intermediate interface in a larger builder pattern. Supports setting the required
   * input sources for reading a RFile.
   *
   * @since 1.8.0
   */
  public static interface InputArguments {
    /**
     * Specify RFiles to read from. When multiple inputs are specified the {@link Scanner}
     * constructed will present a merged view.
     *
     * @param inputs
     *          one or more RFiles to read.
     * @return this
     */
    ScannerOptions from(RFileSource... inputs);

    /**
     * Specify RFiles to read from. When multiple are specified the {@link Scanner} constructed will
     * present a merged view.
     *
     * @param files
     *          one or more RFiles to read.
     * @return this
     */
    ScannerFSOptions from(String... files);
  }

  /**
   * This is an intermediate interface in a larger builder pattern. Enables optionally setting a
   * FileSystem to read RFile(s) from.
   *
   * @since 1.8.0
   */
  public static interface ScannerFSOptions extends ScannerOptions {
    /**
     * Optionally provide a FileSystem to open RFiles. If not specified, the FileSystem will be
     * constructed using configuration on the classpath.
     *
     * @param fs
     *          use this FileSystem to open files.
     * @return this
     */
    ScannerOptions withFileSystem(FileSystem fs);
  }

  /**
   * This is an intermediate interface in a larger builder pattern. Supports setting optional
   * parameters for reading RFile(s) and building a scanner over RFile(s).
   *
   * @since 1.8.0
   */
  public static interface ScannerOptions {

    /**
     * By default the {@link Scanner} created will setup the default Accumulo system iterators. The
     * iterators do things like the following :
     *
     * <ul>
     * <li>Suppress deleted data</li>
     * <li>Filter based on @link {@link Authorizations}</li>
     * <li>Filter columns specified by functions like {@link Scanner#fetchColumn(Text, Text)} and
     * {@link Scanner#fetchColumnFamily(Text)}</li>
     * </ul>
     *
     * <p>
     * Calling this method will turn off these system iterators and allow reading the raw data in an
     * RFile. When reading the raw data, delete data and delete markers may be seen. Delete markers
     * are {@link Key}s with the delete flag set.
     *
     * <p>
     * Disabling system iterators will cause {@link #withAuthorizations(Authorizations)},
     * {@link Scanner#fetchColumn(Text, Text)}, and {@link Scanner#fetchColumnFamily(Text)} to throw
     * runtime exceptions.
     *
     * @return this
     */
    public ScannerOptions withoutSystemIterators();

    /**
     * The authorizations passed here will be used to filter Keys, from the {@link Scanner}, based
     * on the content of the column visibility field.
     *
     * @param auths
     *          scan with these authorizations
     * @return this
     */
    public ScannerOptions withAuthorizations(Authorizations auths);

    /**
     * Enabling this option will cache RFiles data in memory. This option is useful when doing lots
     * of random accesses.
     *
     * @param cacheSize
     *          the size of the data cache in bytes.
     * @return this
     */
    public ScannerOptions withDataCache(long cacheSize);

    /**
     * Enabling this option will cache RFiles indexes in memory. Index data within a RFile is used
     * to find data when seeking to a {@link Key}. This option is useful when doing lots of random
     * accesses.
     *
     * @param cacheSize
     *          the size of the index cache in bytes.
     * @return this
     */
    public ScannerOptions withIndexCache(long cacheSize);

    /**
     * This option allows limiting the {@link Scanner} from reading data outside of a given range. A
     * scanner will not see any data outside of this range even if the RFile(s) have data outside
     * the range.
     *
     * @return this
     */
    public ScannerOptions withBounds(Range range);

    /**
     * Construct the {@link Scanner} with iterators specified in a tables properties. Properties for
     * a table can be obtained by calling {@link TableOperations#getProperties(String)}
     *
     * @param props
     *          iterable over Accumulo table key value properties.
     * @return this
     */
    public ScannerOptions withTableProperties(Iterable<Entry<String,String>> props);

    /**
     * @see #withTableProperties(Iterable)
     * @param props
     *          a map instead of an Iterable
     * @return this
     */
    public ScannerOptions withTableProperties(Map<String,String> props);

    /**
     * @return a Scanner over RFile using the specified options.
     */
    public Scanner build();
  }

  /**
   * Entry point for building a new {@link Scanner} over one or more RFiles.
   */
  public static InputArguments newScanner() {
    return new RFileScannerBuilder();
  }

  /**
   * This is an intermediate interface in a larger builder pattern. Supports setting the required
   * output sink to write a RFile to. The filename parameter requires the ".rf" extension.
   *
   * @since 1.8.0
   */
  public static interface OutputArguments {
    /**
     * @param filename
     *          name of file to write RFile data, ending with the ".rf" extension
     * @return this
     */
    public WriterFSOptions to(String filename);

    /**
     * @param out
     *          output stream to write RFile data
     * @return this
     */
    public WriterOptions to(OutputStream out);
  }

  /**
   * This is an intermediate interface in a larger builder pattern. Enables optionally setting a
   * FileSystem to write to.
   *
   * @since 1.8.0
   */
  public static interface WriterFSOptions extends WriterOptions {
    /**
     * Optionally provide a FileSystem to open a file to write a RFile. If not specified, the
     * FileSystem will be constructed using configuration on the classpath.
     *
     * @param fs
     *          use this FileSystem to open files.
     * @return this
     */
    WriterOptions withFileSystem(FileSystem fs);
  }

  /**
   * This is an intermediate interface in a larger builder pattern. Supports setting optional
   * parameters for creating a RFile and building a RFileWriter.
   *
   * @since 1.8.0
   */
  public static interface WriterOptions {
    /**
     * An option to store sample data in the generated RFile.
     *
     * @param samplerConf
     *          configuration to use when generating sample data.
     * @throws IllegalArgumentException
     *           if table properties were previously specified and the table properties also specify
     *           a sampler.
     * @return this
     */
    public WriterOptions withSampler(SamplerConfiguration samplerConf);

    /**
     * Create an RFile using the same configuration as an Accumulo table. Properties for a table can
     * be obtained by calling {@link TableOperations#getProperties(String)}
     *
     * @param props
     *          iterable over Accumulo table key value properties.
     * @throws IllegalArgumentException
     *           if sampler was previously specified and the table properties also specify a
     *           sampler.
     * @return this
     */
    public WriterOptions withTableProperties(Iterable<Entry<String,String>> props);

    /**
     * @see #withTableProperties(Iterable)
     */
    public WriterOptions withTableProperties(Map<String,String> props);

    /**
     * @param maxSize
     *          As keys are added to an RFile the visibility field is validated. Validating the
     *          visibility field requires parsing it. In order to make validation faster, previously
     *          seen visibilities are cached. This option allows setting the maximum size of this
     *          cache.
     * @return this
     */
    public WriterOptions withVisibilityCacheSize(int maxSize);

    /**
     * @return a new RfileWriter created with the options previously specified.
     */
    public RFileWriter build() throws IOException;
  }

  /**
   * Entry point for creating a new RFile writer.
   */
  public static OutputArguments newWriter() {
    return new RFileWriterBuilder();
  }
}
