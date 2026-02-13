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
package org.apache.accumulo.server.conf;

import org.apache.accumulo.core.cli.ServerOpts;
import org.apache.accumulo.server.ServerDirs;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.util.ServerKeywordExecutable;
import org.apache.accumulo.server.util.adminCommand.SystemCheck.Check;
import org.apache.accumulo.start.spi.CommandGroup;
import org.apache.accumulo.start.spi.CommandGroups;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.hadoop.conf.Configuration;

import com.beust.jcommander.JCommander;
import com.google.auto.service.AutoService;

@AutoService(KeywordExecutable.class)
public class CheckAccumuloProperties extends ServerKeywordExecutable<ServerOpts> {

  public CheckAccumuloProperties() {
    super(new ServerOpts());
  }

  @Override
  public String keyword() {
    return "check-accumulo-properties";
  }

  @Override
  public String description() {
    return "Checks the provided Accumulo configuration file for errors. "
        + "This only checks the contents of the file and not any running Accumulo system, "
        + "so it can be used prior to init, but only performs a subset of the checks done by "
        + "'admin check run " + Check.SERVER_CONFIG + "'";
  }

  @Override
  public CommandGroup commandGroup() {
    return CommandGroups.OTHER;
  }

  @Override
  public void execute(JCommander cl, ServerOpts options) throws Exception {
    var hadoopConfig = new Configuration();
    var siteConfig = options.getSiteConfiguration();

    VolumeManagerImpl.get(siteConfig, hadoopConfig);
    new ServerDirs(siteConfig, hadoopConfig);
  }

}
