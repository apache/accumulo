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

package org.apache.accumulo.tserver.compaction.strategies;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.accumulo.core.compaction.CompactionSettings;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.server.fs.FileRef;
import org.apache.accumulo.tserver.compaction.CompactionPlan;
import org.apache.accumulo.tserver.compaction.CompactionStrategy;
import org.apache.accumulo.tserver.compaction.MajorCompactionRequest;
import org.apache.accumulo.tserver.compaction.WriteParameters;
import org.apache.hadoop.fs.Path;

public class ConfigurableCompactionStrategy extends CompactionStrategy {

  private static abstract class Test {
    // Do any work that blocks in this method. This method is not always called before
    // shouldCompact(). See CompactionStrategy javadocs.
    void gatherInformation(MajorCompactionRequest request) {}

    abstract boolean shouldCompact(Entry<FileRef,DataFileValue> file,
        MajorCompactionRequest request);
  }

  private static class NoSampleTest extends Test {

    private Set<FileRef> filesWithSample = Collections.emptySet();
    private boolean samplingConfigured = true;
    private boolean gatherCalled = false;

    @Override
    void gatherInformation(MajorCompactionRequest request) {
      gatherCalled = true;

      SamplerConfigurationImpl sc = SamplerConfigurationImpl
          .newSamplerConfig(new ConfigurationCopy(request.getTableProperties()));
      if (sc == null) {
        samplingConfigured = false;
      } else {
        filesWithSample = new HashSet<>();
        for (FileRef fref : request.getFiles().keySet()) {
          try (FileSKVIterator reader = request.openReader(fref)) {
            if (reader.getSample(sc) != null) {
              filesWithSample.add(fref);
            }
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }

    @Override
    public boolean shouldCompact(Entry<FileRef,DataFileValue> file,
        MajorCompactionRequest request) {

      if (!gatherCalled) {
        SamplerConfigurationImpl sc = SamplerConfigurationImpl
            .newSamplerConfig(new ConfigurationCopy(request.getTableProperties()));
        return sc != null;
      }

      if (!samplingConfigured) {
        return false;
      }

      return !filesWithSample.contains(file.getKey());
    }
  }

  private static abstract class FileSizeTest extends Test {
    private final long esize;

    private FileSizeTest(String s) {
      this.esize = Long.parseLong(s);
    }

    @Override
    public boolean shouldCompact(Entry<FileRef,DataFileValue> file,
        MajorCompactionRequest request) {
      return shouldCompact(file.getValue().getSize(), esize);
    }

    public abstract boolean shouldCompact(long fsize, long esize);
  }

  private static abstract class PatternPathTest extends Test {
    private Pattern pattern;

    private PatternPathTest(String p) {
      this.pattern = Pattern.compile(p);
    }

    @Override
    public boolean shouldCompact(Entry<FileRef,DataFileValue> file,
        MajorCompactionRequest request) {
      return pattern.matcher(getInput(file.getKey().path())).matches();
    }

    public abstract String getInput(Path path);

  }

  private List<Test> tests = new ArrayList<>();
  private boolean andTest = true;
  private int minFiles = 1;
  private WriteParameters writeParams = new WriteParameters();

  @Override
  public void init(Map<String,String> options) {

    Set<Entry<String,String>> es = options.entrySet();
    for (Entry<String,String> entry : es) {

      switch (CompactionSettings.valueOf(entry.getKey())) {
        case SF_NO_SAMPLE:
          tests.add(new NoSampleTest());
          break;
        case SF_LT_ESIZE_OPT:
          tests.add(new FileSizeTest(entry.getValue()) {
            @Override
            public boolean shouldCompact(long fsize, long esize) {
              return fsize < esize;
            }
          });
          break;
        case SF_GT_ESIZE_OPT:
          tests.add(new FileSizeTest(entry.getValue()) {
            @Override
            public boolean shouldCompact(long fsize, long esize) {
              return fsize > esize;
            }
          });
          break;
        case SF_NAME_RE_OPT:
          tests.add(new PatternPathTest(entry.getValue()) {
            @Override
            public String getInput(Path path) {
              return path.getName();
            }
          });
          break;
        case SF_PATH_RE_OPT:
          tests.add(new PatternPathTest(entry.getValue()) {
            @Override
            public String getInput(Path path) {
              return path.toString();
            }
          });
          break;
        case MIN_FILES_OPT:
          minFiles = Integer.parseInt(entry.getValue());
          break;
        case OUTPUT_COMPRESSION_OPT:
          writeParams.setCompressType(entry.getValue());
          break;
        case OUTPUT_BLOCK_SIZE_OPT:
          writeParams.setBlockSize(Long.parseLong(entry.getValue()));
          break;
        case OUTPUT_INDEX_BLOCK_SIZE_OPT:
          writeParams.setIndexBlockSize(Long.parseLong(entry.getValue()));
          break;
        case OUTPUT_HDFS_BLOCK_SIZE_OPT:
          writeParams.setHdfsBlockSize(Long.parseLong(entry.getValue()));
          break;
        case OUTPUT_REPLICATION_OPT:
          writeParams.setReplication(Integer.parseInt(entry.getValue()));
          break;
        default:
          throw new IllegalArgumentException("Unknown option " + entry.getKey());
      }
    }
  }

  private List<FileRef> getFilesToCompact(MajorCompactionRequest request) {
    List<FileRef> filesToCompact = new ArrayList<>();

    for (Entry<FileRef,DataFileValue> entry : request.getFiles().entrySet()) {
      boolean compact = false;
      for (Test test : tests) {
        if (andTest) {
          compact = test.shouldCompact(entry, request);
          if (!compact)
            break;
        } else {
          compact |= test.shouldCompact(entry, request);
        }
      }

      if (compact || tests.isEmpty())
        filesToCompact.add(entry.getKey());
    }
    return filesToCompact;
  }

  @Override
  public boolean shouldCompact(MajorCompactionRequest request) throws IOException {
    return getFilesToCompact(request).size() >= minFiles;
  }

  @Override
  public void gatherInformation(MajorCompactionRequest request) throws IOException {
    // Gather any information that requires blocking calls here. This is only called before
    // getCompactionPlan() is called.
    for (Test test : tests) {
      test.gatherInformation(request);
    }
  }

  @Override
  public CompactionPlan getCompactionPlan(MajorCompactionRequest request) throws IOException {
    List<FileRef> filesToCompact = getFilesToCompact(request);
    if (filesToCompact.size() >= minFiles) {
      CompactionPlan plan = new CompactionPlan();
      plan.inputFiles.addAll(filesToCompact);
      plan.writeParameters = writeParams;

      return plan;
    }
    return null;
  }
}
