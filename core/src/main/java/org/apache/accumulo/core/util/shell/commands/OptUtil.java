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

import java.io.UnsupportedEncodingException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.hadoop.io.Text;

public abstract class OptUtil {
  public static final String START_ROW_OPT = "b";
  public static final String END_ROW_OPT = "e";

  public static String getTableOpt(final CommandLine cl, final Shell shellState) throws TableNotFoundException {
    String tableName;

    if (cl.hasOption(Shell.tableOption)) {
      tableName = cl.getOptionValue(Shell.tableOption);
      if (!shellState.getConnector().tableOperations().exists(tableName)) {
        throw new TableNotFoundException(tableName, tableName, "specified table that doesn't exist");
      }
    } else {
      shellState.checkTableState();
      tableName = shellState.getTableName();
    }

    return tableName;
  }

  public static String getNamespaceOpt(final CommandLine cl, final Shell shellState) throws NamespaceNotFoundException, AccumuloException,
      AccumuloSecurityException {
    String namespace = null;
    if (cl.hasOption(Shell.namespaceOption)) {
      namespace = cl.getOptionValue(Shell.namespaceOption);
      if (!shellState.getConnector().namespaceOperations().exists(namespace)) {
        throw new NamespaceNotFoundException(namespace, namespace, "specified namespace that doesn't exist");
      }
    } else {
      throw new NamespaceNotFoundException(null, null, "no namespace specified");
    }
    return namespace;
  }

  public static Option tableOpt() {
    return tableOpt("tableName");
  }

  public static Option tableOpt(final String description) {
    final Option tableOpt = new Option(Shell.tableOption, "table", true, description);
    tableOpt.setArgName("table");
    tableOpt.setRequired(false);
    return tableOpt;
  }

  public static Option namespaceOpt() {
    return namespaceOpt("namespace");
  }

  public static Option namespaceOpt(final String description) {
    final Option namespaceOpt = new Option(Shell.namespaceOption, "namespace", true, description);
    namespaceOpt.setArgName("namespace");
    namespaceOpt.setRequired(false);
    return namespaceOpt;
  }

  public static enum AdlOpt {
    ADD("a"), DELETE("d"), LIST("l");

    public final String opt;

    private AdlOpt(String opt) {
      this.opt = opt;
    }
  }

  public static AdlOpt getAldOpt(final CommandLine cl) {
    if (cl.hasOption(AdlOpt.ADD.opt)) {
      return AdlOpt.ADD;
    } else if (cl.hasOption(AdlOpt.DELETE.opt)) {
      return AdlOpt.DELETE;
    } else {
      return AdlOpt.LIST;
    }
  }

  public static OptionGroup addListDeleteGroup(final String name) {
    final Option addOpt = new Option(AdlOpt.ADD.opt, "add", false, "add " + name);
    final Option deleteOpt = new Option(AdlOpt.DELETE.opt, "delete", false, "delete " + name);
    final Option listOpt = new Option(AdlOpt.LIST.opt, "list", false, "list " + name + "(s)");
    final OptionGroup og = new OptionGroup();
    og.addOption(addOpt);
    og.addOption(deleteOpt);
    og.addOption(listOpt);
    og.setRequired(true);
    return og;
  }

  public static Option startRowOpt() {
    final Option o = new Option(START_ROW_OPT, "begin-row", true, "begin row (inclusive)");
    o.setArgName("begin-row");
    return o;
  }

  public static Option endRowOpt() {
    final Option o = new Option(END_ROW_OPT, "end-row", true, "end row (inclusive)");
    o.setArgName("end-row");
    return o;
  }

  public static Text getStartRow(final CommandLine cl) throws UnsupportedEncodingException {
    if (cl.hasOption(START_ROW_OPT)) {
      return new Text(cl.getOptionValue(START_ROW_OPT).getBytes(Shell.CHARSET));
    } else {
      return null;
    }
  }

  public static Text getEndRow(final CommandLine cl) throws UnsupportedEncodingException {
    if (cl.hasOption(END_ROW_OPT)) {
      return new Text(cl.getOptionValue(END_ROW_OPT).getBytes(Shell.CHARSET));
    } else {
      return null;
    }
  }
}
