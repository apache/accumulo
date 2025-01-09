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
package org.apache.accumulo.core.file.rfile;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toCollection;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.cli.ConfigOpts;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.crypto.CryptoFactoryLoader;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.MultiIterator;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.datasketches.quantiles.ItemsSketch;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;
import org.apache.datasketches.quantilescommon.QuantilesUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@AutoService(KeywordExecutable.class)
@SuppressFBWarnings(value = "PATH_TRAVERSAL_OUT",
    justification = "app is run in same security context as user providing the filename")
public class GenerateSplits implements KeywordExecutable {
  private static final Logger log = LoggerFactory.getLogger(GenerateSplits.class);

  private static final Set<Character> allowedChars = new HashSet<>();

  private static final String encodeFlag = "-b64";

  static class Opts extends ConfigOpts {
    @Parameter(names = {"-n", "--num"},
        description = "The number of split points to generate. Can be used to create n+1 tablets. Cannot use with the split size option.")
    public int numSplits = 0;

    @Parameter(names = {"-ss", "--split-size"},
        description = "The minimum split size in uncompressed bytes. Cannot use with num splits option.")
    public long splitSize = 0;

    @Parameter(names = {encodeFlag, "--base64encoded"},
        description = "Base 64 encode the split points")
    public boolean base64encode = false;

    @Parameter(names = {"-sf", "--splits-file"}, description = "Output the splits to a file")
    public String outputFile;

    @Parameter(description = "<file|directory>[ <file|directory>...] -n <num> | -ss <split_size>")
    public List<String> files = new ArrayList<>();

  }

  @Override
  public String keyword() {
    return "generate-splits";
  }

  @Override
  public String description() {
    return "Generate split points from a set of 1 or more rfiles";
  }

  public static void main(String[] args) throws Exception {
    new GenerateSplits().execute(args);
  }

  @Override
  public void execute(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs(GenerateSplits.class.getName(), args);
    if (opts.files.isEmpty()) {
      throw new IllegalArgumentException("No files were given");
    }

    Configuration hadoopConf = new Configuration();
    SiteConfiguration siteConf = opts.getSiteConfiguration();
    CryptoService cryptoService = CryptoFactoryLoader
        .getServiceForClient(CryptoEnvironment.Scope.TABLE, siteConf.getAllCryptoProperties());
    boolean encode = opts.base64encode;

    TreeSet<String> splits;

    if (opts.numSplits > 0 && opts.splitSize > 0) {
      throw new IllegalArgumentException("Requested number of splits and split size.");
    }
    if (opts.numSplits == 0 && opts.splitSize == 0) {
      throw new IllegalArgumentException("Required number of splits or split size.");
    }
    int requestedNumSplits = opts.numSplits;
    long splitSize = opts.splitSize;

    FileSystem fs = FileSystem.get(hadoopConf);
    List<Path> filePaths = new ArrayList<>();
    for (String file : opts.files) {
      Path path = new Path(file);
      fs = PrintInfo.resolveFS(log, hadoopConf, path);
      // get all the files in the directory
      filePaths.addAll(getFiles(fs, path));
    }

    if (filePaths.isEmpty()) {
      throw new IllegalArgumentException("No files were found in " + opts.files);
    } else {
      log.trace("Found the following files: {}", filePaths);
    }

    if (!encode) {
      // Generate the allowed Character set
      for (int i = 0; i < 10; i++) {
        // 0-9
        allowedChars.add((char) (i + 48));
      }
      for (int i = 0; i < 26; i++) {
        // Uppercase A-Z
        allowedChars.add((char) (i + 65));
        // Lowercase a-z
        allowedChars.add((char) (i + 97));
      }
    }

    // if no size specified look at indexed keys first
    if (opts.splitSize == 0) {
      splits = getIndexKeys(siteConf, hadoopConf, fs, filePaths, requestedNumSplits, encode,
          cryptoService);
      // if there weren't enough splits indexed, try again with size = 0
      if (splits.size() < requestedNumSplits) {
        log.info("Only found {} indexed keys but need {}. Doing a full scan on files {}",
            splits.size(), requestedNumSplits, filePaths);
        splits = getSplitsFromFullScan(siteConf, hadoopConf, filePaths, fs, requestedNumSplits,
            encode, cryptoService);
      }
    } else {
      splits =
          getSplitsBySize(siteConf, hadoopConf, filePaths, fs, splitSize, encode, cryptoService);
    }

    TreeSet<String> desiredSplits;
    int numFound = splits.size();
    // its possible we found too many indexed so get requested number but evenly spaced
    if (opts.splitSize == 0 && numFound > requestedNumSplits) {
      desiredSplits = getEvenlySpacedSplits(numFound, requestedNumSplits, splits.iterator());
    } else {
      if (numFound < requestedNumSplits) {
        log.warn("Only found {} splits", numFound);
      }
      desiredSplits = splits;
    }
    log.info("Generated {} splits", desiredSplits.size());
    if (opts.outputFile != null) {
      log.info("Writing splits to file {} ", opts.outputFile);
      try (var writer = new PrintWriter(new BufferedWriter(
          new OutputStreamWriter(new FileOutputStream(opts.outputFile), UTF_8)))) {
        desiredSplits.forEach(writer::println);
      }
    } else {
      desiredSplits.forEach(System.out::println);
    }
  }

  private List<Path> getFiles(FileSystem fs, Path path) throws IOException {
    List<Path> filePaths = new ArrayList<>();
    if (fs.getFileStatus(path).isDirectory()) {
      var iter = fs.listFiles(path, true);
      while (iter.hasNext()) {
        filePaths.addAll(getFiles(fs, iter.next().getPath()));
      }
    } else {
      if (!path.toString().endsWith(".rf")) {
        throw new IllegalArgumentException("Provided file (" + path + ") does not end with '.rf'");
      }
      filePaths.add(path);
    }
    return filePaths;
  }

  private Text[] getQuantiles(SortedKeyValueIterator<Key,Value> iterator, int numSplits)
      throws IOException {
    var itemsSketch = ItemsSketch.getInstance(Text.class, BinaryComparable::compareTo);
    while (iterator.hasTop()) {
      Text row = iterator.getTopKey().getRow();
      itemsSketch.update(row);
      iterator.next();
    }
    // the number requested represents the number of regions between the resulting array elements
    // the actual number of array elements is one more than that to account for endpoints;
    // so, we ask for one more because we want the number of median elements in the array to
    // represent the number of split points and we will drop the first and last array element
    double[] ranks = QuantilesUtil.equallyWeightedRanks(numSplits + 1);
    // the choice to use INCLUSIVE or EXCLUSIVE is arbitrary here; EXCLUSIVE matches the behavior
    // of datasketches 3.x, so we might as well preserve that for 4.x
    Text[] items = itemsSketch.getQuantiles(ranks, QuantileSearchCriteria.EXCLUSIVE);
    // drop the min and max, so we only keep the median elements to use as split points
    return Arrays.copyOfRange(items, 1, items.length - 1);
  }

  /**
   * Return the requested number of splits, evenly spaced across splits found. Visible for testing
   */
  static TreeSet<String> getEvenlySpacedSplits(int numFound, long requestedNumSplits,
      Iterator<String> splitsIter) {
    TreeSet<String> desiredSplits = new TreeSet<>();
    // This is how much each of the found rows will advance towards a desired split point. Add
    // one to numSplits because if we request 9 splits, there will 10 tablets and we want the 9
    // splits evenly spaced between the 10 tablets.
    double increment = (requestedNumSplits + 1.0) / numFound;
    log.debug("Found {} splits but requested {} so picking incrementally by {}", numFound,
        requestedNumSplits, increment);

    // Tracks how far along we are towards the next split.
    double progressToNextSplit = 0;

    for (int i = 0; i < numFound; i++) {
      progressToNextSplit += increment;
      String next = splitsIter.next();
      if (progressToNextSplit > 1 && desiredSplits.size() < requestedNumSplits) {
        desiredSplits.add(next);
        progressToNextSplit -= 1; // decrease by 1 to preserve any partial progress
      }
    }

    return desiredSplits;
  }

  private static String encode(boolean encode, Text text) {
    if (text == null) {
      return null;
    }
    byte[] bytes = TextUtil.getBytes(text);
    if (encode) {
      return Base64.getEncoder().encodeToString(bytes);
    } else {
      StringBuilder sb = new StringBuilder();
      for (byte aByte : bytes) {
        int c = 0xff & aByte;
        if (allowedChars.contains((char) c)) {
          sb.append((char) c);
        } else {
          // Fail if non-printable characters are detected.
          throw new UnsupportedOperationException("Non printable char: \\x" + Integer.toHexString(c)
              + " detected. Must use Base64 encoded output.  The behavior around non printable chars changed in 2.1.3 to throw an error, the previous behavior was likely to cause bugs.");
        }
      }
      return sb.toString();
    }
  }

  /**
   * Scan the files for indexed keys first since it is more efficient than a full file scan.
   */
  private TreeSet<String> getIndexKeys(AccumuloConfiguration accumuloConf, Configuration hadoopConf,
      FileSystem fs, List<Path> files, int requestedNumSplits, boolean base64encode,
      CryptoService cs) throws IOException {
    Text[] splitArray;
    List<SortedKeyValueIterator<Key,Value>> readers = new ArrayList<>(files.size());
    List<FileSKVIterator> fileReaders = new ArrayList<>(files.size());
    try {
      for (Path file : files) {
        FileSKVIterator reader = FileOperations.getInstance().newIndexReaderBuilder()
            .forFile(file.toString(), fs, hadoopConf, cs).withTableConfiguration(accumuloConf)
            .build();
        readers.add(reader);
        fileReaders.add(reader);
      }
      var iterator = new MultiIterator(readers, true);
      splitArray = getQuantiles(iterator, requestedNumSplits);
    } finally {
      for (var r : fileReaders) {
        r.close();
      }
    }

    log.debug("Got {} splits from indices of {}", splitArray.length, files);
    return Arrays.stream(splitArray).map(t -> encode(base64encode, t))
        .collect(toCollection(TreeSet::new));
  }

  private TreeSet<String> getSplitsFromFullScan(SiteConfiguration accumuloConf,
      Configuration hadoopConf, List<Path> files, FileSystem fs, int numSplits,
      boolean base64encode, CryptoService cs) throws IOException {
    Text[] splitArray;
    List<FileSKVIterator> fileReaders = new ArrayList<>(files.size());
    List<SortedKeyValueIterator<Key,Value>> readers = new ArrayList<>(files.size());
    SortedKeyValueIterator<Key,Value> iterator;

    try {
      for (Path file : files) {
        FileSKVIterator reader = FileOperations.getInstance().newScanReaderBuilder()
            .forFile(file.toString(), fs, hadoopConf, cs).withTableConfiguration(accumuloConf)
            .overRange(new Range(), Set.of(), false).build();
        readers.add(reader);
        fileReaders.add(reader);
      }
      iterator = new MultiIterator(readers, false);
      iterator.seek(new Range(), Collections.emptySet(), false);
      splitArray = getQuantiles(iterator, numSplits);
    } finally {
      for (var r : fileReaders) {
        r.close();
      }
    }

    log.debug("Got {} splits from quantiles across {} files", splitArray.length, files.size());
    return Arrays.stream(splitArray).map(t -> encode(base64encode, t))
        .collect(toCollection(TreeSet::new));
  }

  /**
   * Get number of splits based on requested size of split.
   */
  private TreeSet<String> getSplitsBySize(AccumuloConfiguration accumuloConf,
      Configuration hadoopConf, List<Path> files, FileSystem fs, long splitSize,
      boolean base64encode, CryptoService cs) throws IOException {
    long currentSplitSize = 0;
    long totalSize = 0;
    TreeSet<String> splits = new TreeSet<>();
    List<FileSKVIterator> fileReaders = new ArrayList<>(files.size());
    List<SortedKeyValueIterator<Key,Value>> readers = new ArrayList<>(files.size());
    SortedKeyValueIterator<Key,Value> iterator;
    try {
      for (Path file : files) {
        FileSKVIterator reader = FileOperations.getInstance().newScanReaderBuilder()
            .forFile(file.toString(), fs, hadoopConf, cs).withTableConfiguration(accumuloConf)
            .overRange(new Range(), Set.of(), false).build();
        readers.add(reader);
        fileReaders.add(reader);
      }
      iterator = new MultiIterator(readers, false);
      iterator.seek(new Range(), Collections.emptySet(), false);
      while (iterator.hasTop()) {
        Key key = iterator.getTopKey();
        Value val = iterator.getTopValue();
        int size = key.getSize() + val.getSize();
        currentSplitSize += size;
        totalSize += size;
        if (currentSplitSize > splitSize) {
          splits.add(encode(base64encode, key.getRow()));
          currentSplitSize = 0;
        }
        iterator.next();
      }
    } finally {
      for (var r : fileReaders) {
        r.close();
      }
    }

    log.debug("Got {} splits with split size {} out of {} total bytes read across {} files",
        splits.size(), splitSize, totalSize, files.size());
    return splits;
  }
}
