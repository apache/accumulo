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
package org.apache.accumulo.tserver.log;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.log4j.Logger;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.annotations.VisibleForTesting;

/**
 * This class will attempt to rewrite any local WALs to HDFS.
 */
public class LocalWALRecovery implements Runnable {
  private static final Logger log = Logger.getLogger(LocalWALRecovery.class);

  public static void main(String[] args) throws IOException {
    AccumuloConfiguration configuration = SiteConfiguration.getInstance(SiteConfiguration.getDefaultConfiguration());

    LocalWALRecovery main = new LocalWALRecovery(configuration);
    main.parseArgs(args);
    main.run();
  }

  public final class Options {
    @Parameter(names = "--delete-local", description = "Specify whether to delete the local WAL files after they have been re-written in HDFS.")
    public boolean deleteLocal = false;

    @Parameter(names = "--local-wal-directories",
        description = "Comma separated list of local directories containing WALs, default is set according to the logger.dir.walog property.")
    public List<String> directories = getDefaultDirectories();

    @Parameter(names = "--dfs-wal-directory",
        description = "The directory that WALs will be copied into. Will default to the first configured base dir + '/wal'")
    public String destination = null;

    private List<String> getDefaultDirectories() {
      String property = configuration.get(Property.LOGGER_DIR);
      return Arrays.asList(property.split(","));
    }
  }

  private final AccumuloConfiguration configuration;
  private final Options options;

  /**
   * Create a WAL recovery tool for the given instance.
   */
  public LocalWALRecovery(AccumuloConfiguration configuration) {
    this.configuration = configuration;
    this.options = new Options();
  }

  @VisibleForTesting
  public void parseArgs(String... args) {
    JCommander jcommander = new JCommander();
    jcommander.addObject(options);

    try {
      jcommander.parse(args);
    } catch (ParameterException e) {
      jcommander.usage();
    }
  }

  @Override
  public void run() {
    SecurityUtil.serverLogin(ServerConfiguration.getSiteConfiguration());

    try {
      recoverLocalWriteAheadLogs(VolumeManagerImpl.get().getDefaultVolume().getFileSystem());
    } catch (IOException e) {
      log.error("Error while recovering WAL files.", e);
    }
  }

  public void recoverLocalWriteAheadLogs(FileSystem fs) throws IOException {
    for (String directory : options.directories) {
      File localDirectory = new File(directory);
      if (!localDirectory.isAbsolute()) {
        localDirectory = new File(System.getenv("ACCUMULO_HOME"), directory);
      }

      if (!localDirectory.isDirectory()) {
        log.warn("Local walog dir " + localDirectory.getAbsolutePath() + " does not exist or is not a directory.");
        continue;
      }

      if (options.destination == null) {
        // Defer loading the default value until now because it might require talking to zookeeper.
        options.destination = ServerConstants.getWalDirs()[0];
      }
      log.info("Copying WALs to " + options.destination);

      for (File file : localDirectory.listFiles()) {
        String name = file.getName();
        try {
          UUID.fromString(name);
        } catch (IllegalArgumentException ex) {
          log.info("Ignoring non-log file " + file.getAbsolutePath());
          continue;
        }

        @SuppressWarnings("deprecation")
        org.apache.accumulo.server.logger.LogFileKey key = new org.apache.accumulo.server.logger.LogFileKey();
        @SuppressWarnings("deprecation")
        org.apache.accumulo.server.logger.LogFileValue value = new org.apache.accumulo.server.logger.LogFileValue();

        log.info("Openning local log " + file.getAbsolutePath());

        Path localWal = new Path(file.toURI());
        FileSystem localFs = FileSystem.getLocal(fs.getConf());

        @SuppressWarnings("deprecation")
        Reader reader = new SequenceFile.Reader(localFs, localWal, localFs.getConf());
        // Reader reader = new SequenceFile.Reader(localFs.getConf(), SequenceFile.Reader.file(localWal));
        Path tmp = new Path(options.destination + "/" + name + ".copy");
        FSDataOutputStream writer = fs.create(tmp);
        while (reader.next(key, value)) {
          try {
            key.write(writer);
            value.write(writer);
          } catch (EOFException ex) {
            break;
          }
        }
        writer.close();
        reader.close();
        fs.rename(tmp, new Path(tmp.getParent(), name));

        if (options.deleteLocal) {
          if (file.delete()) {
            log.info("Copied and deleted: " + name);
          } else {
            log.info("Failed to delete: " + name + " (but it is safe for you to delete it manually).");
          }
        } else {
          log.info("Safe to delete: " + name);
        }
      }
    }
  }

}
