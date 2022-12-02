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
package org.apache.accumulo.shell.commands;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.PluginConfig;
import org.apache.accumulo.core.compaction.CompactionSettings;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.ShellUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class CompactCommand extends TableOperation {
  private Option noFlushOption, waitOpt, profileOpt, cancelOpt, strategyOpt, strategyConfigOpt;

  // file selection and file output options
  private Option enameOption, epathOption, sizeLtOption, sizeGtOption, minFilesOption,
      outBlockSizeOpt, outHdfsBlockSizeOpt, outIndexBlockSizeOpt, outCompressionOpt, outReplication,
      enoSampleOption, extraSummaryOption, enoSummaryOption, hintsOption, configurerOpt,
      configurerConfigOpt, selectorOpt, selectorConfigOpt;

  private CompactionConfig compactionConfig = null;

  private boolean cancel = false;

  @Override
  public String description() {
    return "Initiates a major compaction on tablets within the specified range"
        + " that have one or more files. If no file selection options are"
        + " specified, then all files will be compacted. Options that configure"
        + " output settings are only applied to this compaction and not later"
        + " compactions. If multiple concurrent user initiated compactions specify"
        + " iterators or a compaction strategy, then all but one will fail to start.";
  }

  @Override
  protected void doTableOp(final Shell shellState, final String tableName)
      throws AccumuloException, AccumuloSecurityException {
    // compact the tables

    if (cancel) {
      try {
        shellState.getAccumuloClient().tableOperations().cancelCompaction(tableName);
        Shell.log.info("Compaction canceled for table {}", tableName);
      } catch (TableNotFoundException e) {
        throw new AccumuloException(e);
      }
    } else {
      try {
        if (compactionConfig.getWait()) {
          Shell.log.info("Compacting table ...");
        }

        for (IteratorSetting iteratorSetting : compactionConfig.getIterators()) {
          ScanCommand.ensureTserversCanLoadIterator(shellState, tableName,
              iteratorSetting.getIteratorClass());
        }

        shellState.getAccumuloClient().tableOperations().compact(tableName, compactionConfig);

        Shell.log.info("Compaction of table {} {} for given range", tableName,
            compactionConfig.getWait() ? "completed" : "started");
      } catch (Exception ex) {
        throw new AccumuloException(ex);
      }
    }
  }

  private void put(CommandLine cl, Map<String,String> sopts, Map<String,String> copts, Option opt,
      CompactionSettings setting) {
    if (cl.hasOption(opt.getLongOpt())) {
      setting.put(sopts, copts, cl.getOptionValue(opt.getLongOpt()));
    }
  }

  private void setupConfigurableCompaction(CommandLine cl, CompactionConfig compactionConfig) {
    Map<String,String> sopts = new HashMap<>();
    Map<String,String> copts = new HashMap<>();

    put(cl, sopts, copts, extraSummaryOption, CompactionSettings.SF_EXTRA_SUMMARY);
    put(cl, sopts, copts, enoSummaryOption, CompactionSettings.SF_NO_SUMMARY);
    put(cl, sopts, copts, enoSampleOption, CompactionSettings.SF_NO_SAMPLE);
    put(cl, sopts, copts, enameOption, CompactionSettings.SF_NAME_RE_OPT);
    put(cl, sopts, copts, epathOption, CompactionSettings.SF_PATH_RE_OPT);
    put(cl, sopts, copts, sizeLtOption, CompactionSettings.SF_LT_ESIZE_OPT);
    put(cl, sopts, copts, sizeGtOption, CompactionSettings.SF_GT_ESIZE_OPT);
    put(cl, sopts, copts, minFilesOption, CompactionSettings.MIN_FILES_OPT);
    put(cl, sopts, copts, outCompressionOpt, CompactionSettings.OUTPUT_COMPRESSION_OPT);
    put(cl, sopts, copts, outBlockSizeOpt, CompactionSettings.OUTPUT_BLOCK_SIZE_OPT);
    put(cl, sopts, copts, outHdfsBlockSizeOpt, CompactionSettings.OUTPUT_HDFS_BLOCK_SIZE_OPT);
    put(cl, sopts, copts, outIndexBlockSizeOpt, CompactionSettings.OUTPUT_INDEX_BLOCK_SIZE_OPT);
    put(cl, sopts, copts, outReplication, CompactionSettings.OUTPUT_REPLICATION_OPT);

    if ((!sopts.isEmpty() || !copts.isEmpty()) && (cl.hasOption(strategyOpt.getOpt())
        || cl.hasOption(selectorOpt.getLongOpt()) || cl.hasOption(configurerOpt.getLongOpt()))) {
      throw new IllegalArgumentException(
          "Can not specify compaction strategy/selector/configurer with file selection and file output options.");
    }

    if (!sopts.isEmpty()) {
      PluginConfig selectorCfg = new PluginConfig(
          "org.apache.accumulo.tserver.compaction.strategies.ConfigurableCompactionStrategy",
          sopts);
      compactionConfig.setSelector(selectorCfg);
    }

    if (!copts.isEmpty()) {
      PluginConfig configurerConfig = new PluginConfig(
          "org.apache.accumulo.tserver.compaction.strategies.ConfigurableCompactionStrategy",
          copts);
      compactionConfig.setConfigurer(configurerConfig);
    }
  }

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState)
      throws Exception {

    if (cl.hasOption(cancelOpt.getLongOpt())) {
      cancel = true;

      if (cl.getOptions().length > 2) {
        throw new IllegalArgumentException("Can not specify other options with cancel");
      }
    } else {
      cancel = false;
    }

    compactionConfig = new CompactionConfig();

    compactionConfig.setFlush(!cl.hasOption(noFlushOption.getOpt()));
    compactionConfig.setWait(cl.hasOption(waitOpt.getOpt()));
    compactionConfig.setStartRow(OptUtil.getStartRow(cl));
    compactionConfig.setEndRow(OptUtil.getEndRow(cl));

    if (cl.hasOption(profileOpt.getOpt())) {
      List<IteratorSetting> iterators =
          shellState.iteratorProfiles.get(cl.getOptionValue(profileOpt.getOpt()));
      if (iterators == null) {
        Shell.log.error("Profile {} does not exist", cl.getOptionValue(profileOpt.getOpt()));
        return -1;
      }

      compactionConfig.setIterators(new ArrayList<>(iterators));
    }

    setupConfigurableCompaction(cl, compactionConfig);

    if (cl.hasOption(strategyOpt.getOpt())) {
      if (cl.hasOption(selectorOpt.getLongOpt()) || cl.hasOption(configurerOpt.getLongOpt())) {
        throw new IllegalArgumentException(
            "Can not specify a strategy with a selector or configurer");
      }
      configureCompactionStrat(cl);
    } else {
      if (cl.hasOption(selectorOpt.getLongOpt())) {
        compactionConfig.setSelector(new PluginConfig(cl.getOptionValue(selectorOpt.getLongOpt()),
            ShellUtil.parseMapOpt(cl, selectorConfigOpt)));
      }

      if (cl.hasOption(configurerOpt.getLongOpt())) {
        compactionConfig
            .setConfigurer(new PluginConfig(cl.getOptionValue(configurerOpt.getLongOpt()),
                ShellUtil.parseMapOpt(cl, configurerConfigOpt)));
      }
    }

    if (cl.hasOption(hintsOption.getLongOpt())) {
      compactionConfig.setExecutionHints(ShellUtil.parseMapOpt(cl, hintsOption));
    }

    return super.execute(fullCommand, cl, shellState);
  }

  @SuppressWarnings("removal")
  private void configureCompactionStrat(final CommandLine cl) {
    var csc = new org.apache.accumulo.core.client.admin.CompactionStrategyConfig(
        cl.getOptionValue(strategyOpt.getOpt()));
    csc.setOptions(ShellUtil.parseMapOpt(cl, strategyConfigOpt));
    compactionConfig.setCompactionStrategy(csc);
  }

  private Option newLAO(String lopt, String desc) {
    return new Option(null, lopt, true, desc);
  }

  @Override
  public Options getOptions() {
    final Options opts = super.getOptions();

    opts.addOption(OptUtil.startRowOpt());
    opts.addOption(OptUtil.endRowOpt());
    noFlushOption =
        new Option("nf", "noFlush", false, "do not flush table data in memory before compacting.");
    opts.addOption(noFlushOption);
    waitOpt = new Option("w", "wait", false, "wait for compact to finish");
    opts.addOption(waitOpt);

    profileOpt = new Option("pn", "profile", true, "Iterator profile name.");
    profileOpt.setArgName("profile");
    opts.addOption(profileOpt);

    strategyOpt = new Option("s", "strategy", true, "compaction strategy class name");
    opts.addOption(strategyOpt);
    strategyConfigOpt = new Option("sc", "strategyConfig", true,
        "Key value options for compaction strategy.  Expects <prop>=<value>{,<prop>=<value>}");
    opts.addOption(strategyConfigOpt);

    hintsOption = newLAO("exec-hints",
        "Compaction execution hints.  Expects <prop>=<value>{,<prop>=<value>}");
    opts.addOption(hintsOption);

    selectorOpt = newLAO("selector", "Class name of a compaction selector.");
    opts.addOption(selectorOpt);
    selectorConfigOpt = newLAO("selectorConfig",
        "Key value options for compaction selector.  Expects <prop>=<value>{,<prop>=<value>}");
    opts.addOption(selectorConfigOpt);

    configurerOpt = newLAO("configurer", "Class name of a compaction configurer.");
    opts.addOption(configurerOpt);
    configurerConfigOpt = newLAO("configurerConfig",
        "Key value options for compaction configurer.  Expects <prop>=<value>{,<prop>=<value>}");
    opts.addOption(configurerConfigOpt);

    cancelOpt = new Option(null, "cancel", false, "cancel user initiated compactions");
    opts.addOption(cancelOpt);

    enoSummaryOption = new Option(null, "sf-no-summary", false,
        "Select files that do not have the summaries specified in the table configuration.");
    opts.addOption(enoSummaryOption);
    extraSummaryOption = new Option(null, "sf-extra-summary", false,
        "Select files that have summary information which exceeds the tablets boundaries.");
    opts.addOption(extraSummaryOption);
    enoSampleOption = new Option(null, "sf-no-sample", false,
        "Select files that have no sample data or sample data that differs"
            + " from the table configuration.");
    opts.addOption(enoSampleOption);
    enameOption =
        newLAO("sf-ename", "Select files using regular expression to match file names. Only"
            + " matches against last part of path.");
    opts.addOption(enameOption);
    epathOption =
        newLAO("sf-epath", "Select files using regular expression to match file paths to compact."
            + " Matches against full path.");
    opts.addOption(epathOption);
    sizeLtOption =
        newLAO("sf-lt-esize", "Selects files less than specified size.  Uses the estimated size of"
            + " file in metadata table. Can use K,M, and G suffixes");
    opts.addOption(sizeLtOption);
    sizeGtOption = newLAO("sf-gt-esize",
        "Selects files greater than specified size. Uses the estimated size of"
            + " file in metadata table. Can use K,M, and G suffixes");
    opts.addOption(sizeGtOption);
    minFilesOption =
        newLAO("min-files", "Only compacts if at least the specified number of files are selected."
            + " When no file selection criteria are given, all files are selected.");
    opts.addOption(minFilesOption);
    outBlockSizeOpt = newLAO("out-data-bs",
        "Rfile data block size to use for compaction output file. Can use K,M,"
            + " and G suffixes. Uses table settings if not specified.");
    opts.addOption(outBlockSizeOpt);
    outHdfsBlockSizeOpt = newLAO("out-hdfs-bs",
        "HDFS block size to use for compaction output file. Can use K,M, and G"
            + " suffixes. Uses table settings if not specified.");
    opts.addOption(outHdfsBlockSizeOpt);
    outIndexBlockSizeOpt =
        newLAO("out-index-bs", "Rfile index block size to use for compaction output file. Can use"
            + " K,M, and G suffixes. Uses table settings if not specified.");
    opts.addOption(outIndexBlockSizeOpt);
    outCompressionOpt = newLAO("out-compress",
        "Compression to use for compaction output file. Either snappy, gz, bzip2, lzo,"
            + " lz4, zstd, or none. Uses table settings if not specified.");
    opts.addOption(outCompressionOpt);
    outReplication =
        newLAO("out-replication", "HDFS replication to use for compaction output file. Uses table"
            + " settings if not specified.");
    opts.addOption(outReplication);

    return opts;
  }
}
