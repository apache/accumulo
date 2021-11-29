/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.cli.ConfigOpts;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.crypto.CryptoServiceFactory;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.MultiIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

  static class Opts extends ConfigOpts {
    @Parameter(names = {"-a", "--auths"}, converter = ClientOpts.AuthConverter.class,
        description = "the authorizations to use when reading the files")
    public Authorizations auths = Authorizations.EMPTY;

    @Parameter(names = {"-n", "--num"}, description = "The number of splits to generate. Cannot use with the split size option.")
    public long numSplits = 0;

    @Parameter(names = {"-ss", "--split-size"}, description = "The split size desired in bytes. Cannot use with num splits option.")
    public long splitSize = 0;

    @Parameter(names = {"-b64", "--base64encoded"}, description = "Base 64 encode the split points")
    public boolean base64encode = false;

    @Parameter(names = {"-sf", "--splits-file"}, description = "Output the splits to a file")
    public String outputFile;

    @Parameter(description = " <file|directory> { <file> ... }")
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
    boolean encode = opts.base64encode;

    TreeSet<String> splits;
    TreeSet<String> desiredSplits = new TreeSet<>();

    if (opts.numSplits > 0 && opts.splitSize > 0) {
      throw new IllegalArgumentException("Requested number of splits and split size.");
    }
    if (opts.numSplits == 0 && opts.splitSize == 0) {
      throw new IllegalArgumentException("Required number of splits or split size.");
    }
    long numSplits = opts.numSplits;
    long splitSize = opts.splitSize;

    FileSystem fs = FileSystem.get(hadoopConf);
    FileSystem localFs = FileSystem.getLocal(hadoopConf);
    List<Path> filePaths = new ArrayList<>();
    for (String file : opts.files) {
      Path path = new Path(file);
      if (file.contains(":")) {
        fs = path.getFileSystem(hadoopConf);
      } else {
        log.warn("Attempting to find file across filesystems. Consider providing URI "
            + "instead of path");
        if (!fs.exists(path))
          fs = localFs; // fall back to local
      }
      // get all the files in the directory
      if (fs.getFileStatus(path).isDirectory()) {
        // can only explode one directory
        if (opts.files.size() > 1)
          throw new IllegalArgumentException("Only one directory can be specified");
        var iter = fs.listFiles(path, true);
        while (iter.hasNext()) {
          filePaths.add(iter.next().getPath());
        }
      } else {
        filePaths.add(path);
      }
    }

    // if no size specified look at indexed keys first
    if (opts.splitSize == 0) {
      splits = getIndexKeys(siteConf, hadoopConf, fs, filePaths, encode);
      // if there weren't enough splits indexed, try again with size = 0
      if (splits.size() < numSplits) {
        log.info("Only found {} indexed keys but need {}. Doing a full scan on files {}",
            splits.size(), numSplits, filePaths);
        splits = getSplitsBySize(siteConf, hadoopConf, filePaths, fs, 0, encode);
      }
    } else {
      splits = getSplitsBySize(siteConf, hadoopConf, filePaths, fs, splitSize, encode);
    }

    int numFound = splits.size();
    // its possible we found too many indexed so take every (numFound / numSplits) split
    if (opts.splitSize == 0 && numFound > numSplits) {
      var iter = splits.iterator();
      long factor = numFound / numSplits;
      log.debug("Found {} splits but requested {} picking 1 every {}", numFound, opts.numSplits,
          factor);
      for (int i = 0; i < numFound; i++) {
        String next = iter.next();
        if (i % factor == 0 && desiredSplits.size() < numSplits) {
          desiredSplits.add(next);
        }
      }
    } else {
      if (numFound < numSplits)
        log.warn("Only found {} splits", numFound);
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

  private static String encode(boolean encode, Text text) {
    if (text == null) {
      return null;
    }
    byte[] bytes = TextUtil.getBytes(text);
    if (encode)
      return Base64.getEncoder().encodeToString(bytes);
    else {
      // drop non printable characters
      StringBuilder sb = new StringBuilder();
      for (byte aByte : bytes) {
        int c = 0xff & aByte;
        if (c == '\\')
          sb.append("\\\\");
        else if (c >= 32 && c <= 126)
          sb.append((char) c);
        else
          log.debug("Dropping non printable char: \\x{}", Integer.toHexString(c));
      }
      return sb.toString();
    }
  }

  /**
   * Scan the files for indexed keys since it is more efficient than a full file scan.
   */
  private TreeSet<String> getIndexKeys(AccumuloConfiguration accumuloConf, Configuration hadoopConf,
      FileSystem fs, List<Path> files, boolean base64encode) throws IOException {
    TreeSet<String> indexKeys = new TreeSet<>();
    List<SortedKeyValueIterator<Key,Value>> readers = new ArrayList<>(files.size());
    List<FileSKVIterator> fileReaders = new ArrayList<>(files.size());
    try {
      for (Path file : files) {
        FileSKVIterator reader = FileOperations.getInstance().newIndexReaderBuilder()
            .forFile(file.toString(), fs, hadoopConf, CryptoServiceFactory.newDefaultInstance())
            .withTableConfiguration(accumuloConf).build();
        readers.add(reader);
        fileReaders.add(reader);
      }
      var iterator = new MultiIterator(readers, true);
      while (iterator.hasTop()) {
        Key key = iterator.getTopKey();
        indexKeys.add(encode(base64encode, key.getRow()));
        iterator.next();
      }
    } finally {
      for (var r : fileReaders) {
        r.close();
      }
    }

    log.debug("Got {} splits from indices of {}", indexKeys.size(), files);
    return indexKeys;
  }

  /**
   * Get number of splits based on requested size of split. A splitSize = 0 returns all keys.
   */
  private TreeSet<String> getSplitsBySize(AccumuloConfiguration accumuloConf,
      Configuration hadoopConf, List<Path> files, FileSystem fs, long splitSize,
      boolean base64encode) throws IOException {
    long currentSplitSize = 0;
    long totalSize = 0;
    TreeSet<String> splits = new TreeSet<>();
    List<FileSKVIterator> fileReaders = new ArrayList<>(files.size());
    List<SortedKeyValueIterator<Key,Value>> readers = new ArrayList<>(files.size());
    SortedKeyValueIterator<Key,Value> iterator;
    try {
      for (Path file : files) {
        FileSKVIterator reader = FileOperations.getInstance().newScanReaderBuilder()
            .forFile(file.toString(), fs, hadoopConf, CryptoServiceFactory.newDefaultInstance())
            .withTableConfiguration(accumuloConf).overRange(new Range(), Set.of(), false).build();
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
