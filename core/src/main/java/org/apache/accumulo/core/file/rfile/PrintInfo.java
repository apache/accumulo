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
package org.apache.accumulo.core.file.rfile;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.accumulo.core.file.rfile.RFile.Reader;
import org.apache.accumulo.core.summary.SummaryReader;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;

@AutoService(KeywordExecutable.class)
public class PrintInfo implements KeywordExecutable {

  private static final Logger log = LoggerFactory.getLogger(PrintInfo.class);

  static class Opts extends Help {
    @Parameter(names = {"-d", "--dump"}, description = "dump the key/value pairs")
    boolean dump = false;
    @Parameter(names = {"-v", "--vis"}, description = "show visibility metrics")
    boolean vis = false;
    @Parameter(names = {"--visHash"}, description = "show visibilities as hashes, implies -v")
    boolean hash = false;
    @Parameter(names = {"--histogram"}, description = "print a histogram of the key-value sizes")
    boolean histogram = false;
    @Parameter(names = {"--printIndex"}, description = "prints information about all the index entries")
    boolean printIndex = false;
    @Parameter(names = {"--useSample"}, description = "Use sample data for --dump, --vis, --histogram options")
    boolean useSample = false;
    @Parameter(names = {"--summary"}, description = "Print summary data in file")
    boolean printSummary = false;
    @Parameter(names = {"--keyStats"}, description = "print key length statistics for index and all data")
    boolean keyStats = false;
    @Parameter(description = " <file> { <file> ... }")
    List<String> files = new ArrayList<>();
    @Parameter(names = {"-c", "--config"}, variableArity = true, description = "Comma-separated Hadoop configuration files")
    List<String> configFiles = new ArrayList<>();
  }

  static class LogHistogram {
    long countBuckets[] = new long[11];
    long sizeBuckets[] = new long[countBuckets.length];
    long totalSize = 0;

    public void add(int size) {
      int bucket = (int) Math.log10(size);
      countBuckets[bucket]++;
      sizeBuckets[bucket] += size;
      totalSize += size;
    }

    public void print(String indent) {
      System.out.println(indent + "Up to size      count      %-age");
      for (int i = 1; i < countBuckets.length; i++) {
        System.out.println(String.format("%s%11.0f : %10d %6.2f%%", indent, Math.pow(10, i), countBuckets[i], sizeBuckets[i] * 100. / totalSize));
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
      System.out.printf("%smin:%,11.2f max:%,11.2f avg:%,11.2f stddev:%,11.2f\n", indent, stats.getMin(), stats.getMax(), stats.getMean(),
          stats.getStandardDeviation());
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

  @Override
  public void execute(final String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs("accumulo rfile-info", args);
    if (opts.files.isEmpty()) {
      System.err.println("No files were given");
      System.exit(-1);
    }

    Configuration conf = new Configuration();
    for (String confFile : opts.configFiles) {
      log.debug("Adding Hadoop configuration file {}", confFile);
      conf.addResource(new Path(confFile));
    }

    FileSystem hadoopFs = FileSystem.get(conf);
    FileSystem localFs = FileSystem.getLocal(conf);

    LogHistogram kvHistogram = new LogHistogram();

    KeyStats dataKeyStats = new KeyStats();
    KeyStats indexKeyStats = new KeyStats();

    for (String arg : opts.files) {
      Path path = new Path(arg);
      FileSystem fs;
      if (arg.contains(":"))
        fs = path.getFileSystem(conf);
      else {
        log.warn("Attempting to find file across filesystems. Consider providing URI instead of path");
        fs = hadoopFs.exists(path) ? hadoopFs : localFs; // fall back to local
      }
      System.out.println("Reading file: " + path.makeQualified(fs.getUri(), fs.getWorkingDirectory()).toString());

      CachableBlockFile.Reader _rdr = new CachableBlockFile.Reader(fs, path, conf, null, null, SiteConfiguration.getInstance());
      Reader iter = new RFile.Reader(_rdr);
      MetricsGatherer<Map<String,ArrayList<VisibilityMetric>>> vmg = new VisMetricsGatherer();

      if (opts.vis || opts.hash)
        iter.registerMetrics(vmg);

      iter.printInfo(opts.printIndex);
      System.out.println();
      org.apache.accumulo.core.file.rfile.bcfile.PrintInfo.main(new String[] {arg});

      Map<String,ArrayList<ByteSequence>> localityGroupCF = null;

      if (opts.histogram || opts.dump || opts.vis || opts.hash || opts.keyStats) {
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

        for (String lgName : localityGroupCF.keySet()) {
          LocalityGroupUtil.seek(dataIter, new Range(), lgName, localityGroupCF);
          while (dataIter.hasTop()) {
            Key key = dataIter.getTopKey();
            Value value = dataIter.getTopValue();
            if (opts.dump) {
              System.out.println(key + " -> " + value);
              if (System.out.checkError())
                return;
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
      if (System.out.checkError())
        return;
    }
  }
}
