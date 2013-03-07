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
package org.apache.accumulo.server.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.server.cli.ClientOpts;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.beust.jcommander.Parameter;

public class TableDiskUsage {
  
  static class Opts extends ClientOpts {
    @Parameter(description=" <table> { <table> ... } ")
    List<String> tables = new ArrayList<String>();
  }
  
  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    FileSystem fs = FileSystem.get(new Configuration());
    Opts opts = new Opts();
    opts.parseArgs(TableDiskUsage.class.getName(), args);
    Connector conn = opts.getConnector();
    org.apache.accumulo.core.util.TableDiskUsage.printDiskUsage(DefaultConfiguration.getInstance(), opts.tables, fs, conn);
  }
  
}
