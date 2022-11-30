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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import org.apache.accumulo.core.cli.ConfigOpts;
import org.apache.accumulo.core.crypto.CryptoFactoryLoader;
import org.apache.accumulo.core.crypto.CryptoUtils;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile.CachableBuilder;
import org.apache.accumulo.core.file.rfile.RFile.Reader;
import org.apache.accumulo.core.file.rfile.bcfile.PrintBCInfo;
import org.apache.accumulo.core.file.rfile.bcfile.Utils;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.core.spi.crypto.NoFileEncrypter;
import org.apache.accumulo.core.summary.SummaryReader;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.core.util.NumUtil;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@AutoService(KeywordExecutable.class)
public class PrintInfo implements KeywordExecutable {

  private static final Logger log = LoggerFactory.getLogger(PrintInfo.class);

  static class Opts extends ConfigOpts {
    @Parameter(names = {"-d", "--dump"}, description = "dump the key/value pairs")
    boolean dump = false;
    @Parameter(names = {"--fullKeys"},
        description = "dump full keys regardless of length, do no truncate, implies --dump")
    boolean fullKeys = false;
    @Parameter(names = {"--formatter"},
        description = "specify a BiFunction<Key, Value, String> class to apply to rfile contents, implies --dump")
    String formatterClazz = null;
    @Parameter(names = {"-v", "--vis"}, description = "show visibility metrics")
    boolean vis = false;
    @Parameter(names = {"--visHash"}, description = "show visibilities as hashes, implies -v")
    boolean hash = false;
    @Parameter(names = {"--histogram"}, description = "print a histogram of the key-value sizes")
    boolean histogram = false;
    @Parameter(names = {"--printIndex"},
        description = "prints information about all the index entries")
    boolean printIndex = false;
    @Parameter(names = {"--useSample"},
        description = "Use sample data for --dump, --vis, --histogram options")
    boolean useSample = false;
    @Parameter(names = {"--summary"}, description = "Print summary data in file")
    boolean printSummary = false;
    @Parameter(names = {"--keyStats"},
        description = "print key length statistics for index and all data")
    boolean keyStats = false;
    @Parameter(description = " <file> { <file> ... }")
    List<String> files = new ArrayList<>();
    @Parameter(names = {"-c", "--config"}, variableArity = true,
        description = "Comma-separated Hadoop configuration files")
    List<String> configFiles = new ArrayList<>();
  }

  static class LogHistogram {
    long[] countBuckets = new long[11];
    long[] sizeBuckets = new long[countBuckets.length];
    long totalSize = 0;

    public void add(int size) {
      int bucket = (int) Math.log10(size);
      countBuckets[bucket]++;
      sizeBuckets[bucket] += size;
      totalSize += size;
    }

    public void print(String indent) {
      System.out.println(indent + "Up to size      Count      %-age");
      for (int i = 1; i < countBuckets.length; i++) {
        System.out.printf("%s%11s : %10d %6.2f%%%n", indent,
            NumUtil.bigNumberForQuantity((long) Math.pow(10, i)), countBuckets[i],
            sizeBuckets[i] * 100. / totalSize);
      }
    }
  }

  static class KeyStats {
    private SummaryStatistics stats = new SummaryStatistics();
    private LogHistogram logHistogram = new LogHistogram();

    public void add(Key k) {
      int size = k.getSize();
      stats.addValue(size);
      logHistogram.add(size);
    }

    public void print(String indent) {
      logHistogram.print(indent);
      System.out.println();
      System.out.printf("%smin:%,11.2f max:%,11.2f avg:%,11.2f stddev:%,11.2f\n", indent,
          stats.getMin(), stats.getMax(), stats.getMean(), stats.getStandardDeviation());
    }
  }

  public static void main(String[] args) throws Exception {
    new PrintInfo().execute(args);
  }

  @Override
  public String keyword() {
    return "rfile-info";
  }

  @Override
  public String description() {
    return "Prints rfile info";
  }

  protected Class<? extends BiFunction<Key,Value,String>> getFormatter(String formatterClazz)
      throws ClassNotFoundException {
    @SuppressWarnings("unchecked")
    var clazz = (Class<? extends BiFunction<Key,Value,String>>) this.getClass().getClassLoader()
        .loadClass(formatterClazz).asSubclass(BiFunction.class);
    return clazz;
  }

  @SuppressFBWarnings(value = "DM_EXIT",
      justification = "System.exit is fine here because it's a utility class executed by a main()")
  @Override
  public void execute(final String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs("accumulo rfile-info", args);
    if (opts.files.isEmpty()) {
      System.err.println("No files were given");
      System.exit(1);
    }

    if ((opts.fullKeys || opts.dump) && opts.formatterClazz != null) {
      System.err.println(
          "--formatter argument is incompatible with --dump or --fullKeys, specify either, not both.");
      System.exit(1);
    }

    var siteConfig = opts.getSiteConfiguration();

    Configuration conf = new Configuration();
    for (String confFile : opts.configFiles) {
      log.debug("Adding Hadoop configuration file {}", confFile);
      conf.addResource(new Path(confFile));
    }
    LogHistogram kvHistogram = new LogHistogram();

    KeyStats dataKeyStats = new KeyStats();
    KeyStats indexKeyStats = new KeyStats();

    for (String arg : opts.files) {
      Path path = new Path(arg);
      FileSystem fs = resolveFS(log, conf, path);
      System.out
          .println("Reading file: " + path.makeQualified(fs.getUri(), fs.getWorkingDirectory()));

      printCryptoParams(path, fs);

      CryptoService cs = CryptoFactoryLoader.getServiceForClient(CryptoEnvironment.Scope.TABLE,
          siteConfig.getAllCryptoProperties());
      CachableBuilder cb = new CachableBuilder().fsPath(fs, path).conf(conf).cryptoService(cs);
      Reader iter = new RFile.Reader(cb);
      MetricsGatherer<Map<String,ArrayList<VisibilityMetric>>> vmg = new VisMetricsGatherer();

      if (opts.vis || opts.hash) {
        iter.registerMetrics(vmg);
      }

      iter.printInfo(opts.printIndex);
      System.out.println();
      String propsPath = opts.getPropertiesPath();
      String[] mainArgs =
          propsPath == null ? new String[] {arg} : new String[] {"-props", propsPath, arg};
      PrintBCInfo printBCInfo = new PrintBCInfo(mainArgs);
      printBCInfo.setCryptoService(cs);
      printBCInfo.printMetaBlockInfo();

      Map<String,ArrayList<ByteSequence>> localityGroupCF = null;

      if (opts.histogram || opts.dump || opts.vis || opts.hash || opts.keyStats || opts.fullKeys
          || !StringUtils.isEmpty(opts.formatterClazz)) {
        localityGroupCF = iter.getLocalityGroupCF();

        FileSKVIterator dataIter;
        if (opts.useSample) {
          dataIter = iter.getSample();

          if (dataIter == null) {
            System.out.println("ERROR : This rfile has no sample data");
            return;
          }
        } else {
          dataIter = iter;
        }

        if (opts.keyStats) {
          FileSKVIterator indexIter = iter.getIndex();
          while (indexIter.hasTop()) {
            indexKeyStats.add(indexIter.getTopKey());
            indexIter.next();
          }
        }

        BiFunction<Key,Value,String> formatter = null;
        if (opts.formatterClazz != null) {
          final Class<? extends BiFunction<Key,Value,String>> formatterClass =
              getFormatter(opts.formatterClazz);
          formatter = formatterClass.getConstructor().newInstance();
        } else if (opts.fullKeys) {
          formatter = (key, value) -> key.toStringNoTruncate() + " -> " + value;
        } else if (opts.dump) {
          formatter = (key, value) -> key + " -> " + value;
        }

        for (String lgName : localityGroupCF.keySet()) {
          LocalityGroupUtil.seek(dataIter, new Range(), lgName, localityGroupCF);

          while (dataIter.hasTop()) {
            Key key = dataIter.getTopKey();
            Value value = dataIter.getTopValue();
            if (formatter != null) {
              System.out.println(formatter.apply(key, value));
              if (System.out.checkError()) {
                return;
              }
            }

            if (opts.histogram) {
              kvHistogram.add(key.getSize() + value.getSize());
            }
            if (opts.keyStats) {
              dataKeyStats.add(key);
            }

            dataIter.next();
          }
        }
      }

      if (opts.printSummary) {
        SummaryReader.print(iter, System.out);
      }

      iter.close();

      if (opts.vis || opts.hash) {
        System.out.println();
        vmg.printMetrics(opts.hash, "Visibility", System.out);
      }

      if (opts.histogram) {
        System.out.println();
        kvHistogram.print("");
      }

      if (opts.keyStats) {
        System.out.println();
        System.out.println("Statistics for keys in data :");
        dataKeyStats.print("\t");
        System.out.println();
        System.out.println("Statistics for keys in index :");
        indexKeyStats.print("\t");
      }
      // If the output stream has closed, there is no reason to keep going.
      if (System.out.checkError()) {
        return;
      }
    }
  }

  public static FileSystem resolveFS(Logger log, Configuration conf, Path file) throws IOException {
    FileSystem hadoopFs = FileSystem.get(conf);
    FileSystem localFs = FileSystem.getLocal(conf);
    FileSystem fs;
    if (file.toString().contains(":")) {
      fs = file.getFileSystem(conf);
    } else {
      log.warn(
          "Attempting to find file across filesystems. Consider providing URI instead of path");
      fs = hadoopFs.exists(file) ? hadoopFs : localFs; // fall back to local
    }
    return fs;
  }

  /**
   * Print the unencrypted parameters that tell the Crypto Service how to decrypt the file. This
   * information is useful for debugging if and how a file was encrypted.
   */
  private void printCryptoParams(Path path, FileSystem fs) {
    byte[] noCryptoBytes = new NoFileEncrypter().getDecryptionParameters();
    try (FSDataInputStream fsDis = fs.open(path)) {
      long fileLength = fs.getFileStatus(path).getLen();
      fsDis.seek(fileLength - 16 - Utils.Version.size() - Long.BYTES);
      long cryptoParamOffset = fsDis.readLong();
      fsDis.seek(cryptoParamOffset);
      byte[] cryptoParams = CryptoUtils.readParams(fsDis);
      if (Arrays.equals(noCryptoBytes, cryptoParams)) {
        System.out.println("No on disk encryption detected.");
      } else {
        System.out.println("Encrypted with Params: "
            + Key.toPrintableString(cryptoParams, 0, cryptoParams.length, cryptoParams.length));
      }
    } catch (IOException ioe) {
      log.error("Error reading crypto params", ioe);
    }
  }
}
