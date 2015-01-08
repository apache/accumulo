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
package org.apache.accumulo.core.util.shell.commands;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.Text;

public class CompactCommand extends TableOperation {
  private Option noFlushOption, waitOpt, profileOpt, cancelOpt;
  private boolean flush;
  private Text startRow;
  private Text endRow;
  private List<IteratorSetting> iterators;

  boolean override = false;
  private boolean wait;

  private boolean cancel = false;

  @Override
  public String description() {
    return "sets all tablets for a table to major compact as soon as possible (based on current time)";
  }

  protected void doTableOp(final Shell shellState, final String tableName) throws AccumuloException, AccumuloSecurityException {
    // compact the tables

    if (cancel) {
      try {
        shellState.getConnector().tableOperations().cancelCompaction(tableName);
        Shell.log.info("Compaction canceled for table " + tableName);
      } catch (TableNotFoundException e) {
        throw new AccumuloException(e);
      }
    } else {
      try {
        if (wait) {
          Shell.log.info("Compacting table ...");
        }

        shellState.getConnector().tableOperations().compact(tableName, startRow, endRow, iterators, flush, wait);

        Shell.log.info("Compaction of table " + tableName + " " + (wait ? "completed" : "started") + " for given range");
      } catch (Exception ex) {
        throw new AccumuloException(ex);
      }
    }
  }

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState) throws Exception {

    if (cl.hasOption(cancelOpt.getLongOpt())) {
      cancel = true;

      if (cl.getOptions().length > 2) {
        throw new IllegalArgumentException("Can not specify other options with cancel");
      }
    } else {
      cancel = false;
    }

    flush = !cl.hasOption(noFlushOption.getOpt());
    startRow = OptUtil.getStartRow(cl);
    endRow = OptUtil.getEndRow(cl);
    wait = cl.hasOption(waitOpt.getOpt());

    if (cl.hasOption(profileOpt.getOpt())) {
      List<IteratorSetting> iterators = shellState.iteratorProfiles.get(cl.getOptionValue(profileOpt.getOpt()));
      if (iterators == null) {
        Shell.log.error("Profile " + cl.getOptionValue(profileOpt.getOpt()) + " does not exist");
        return -1;
      }

      this.iterators = new ArrayList<IteratorSetting>(iterators);
    } else {
      this.iterators = Collections.emptyList();
    }

    return super.execute(fullCommand, cl, shellState);
  }

  @Override
  public Options getOptions() {
    final Options opts = super.getOptions();

    opts.addOption(OptUtil.startRowOpt());
    opts.addOption(OptUtil.endRowOpt());
    noFlushOption = new Option("nf", "noFlush", false, "do not flush table data in memory before compacting.");
    opts.addOption(noFlushOption);
    waitOpt = new Option("w", "wait", false, "wait for compact to finish");
    opts.addOption(waitOpt);

    profileOpt = new Option("pn", "profile", true, "iterator profile name");
    profileOpt.setArgName("profile");
    opts.addOption(profileOpt);

    cancelOpt = new Option(null, "cancel", false, "cancel user initiated compactions");
    opts.addOption(cancelOpt);

    return opts;
  }
}
