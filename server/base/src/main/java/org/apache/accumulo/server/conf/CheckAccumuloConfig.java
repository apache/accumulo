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

import java.io.File;
import java.io.IOException;

import com.google.auto.service.AutoService;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.server.ServerDirs;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Preconditions;

@AutoService(KeywordExecutable.class)
public class CheckAccumuloConfig implements KeywordExecutable {

  public static void main(String[] args) {
    var hadoopConfig = new Configuration();
    SiteConfiguration siteConfig;
    if (args.length == 0) {
      siteConfig = SiteConfiguration.auto();
    } else {
      Preconditions.checkArgument(args.length == 1, "Expected 1 argument, got " + args.length);
      String filePath = args[0];
      siteConfig = SiteConfiguration.fromFile(new File(filePath)).build();
    }

    try {
      VolumeManagerImpl.get(siteConfig, hadoopConfig);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    new ServerDirs(siteConfig, hadoopConfig);
  }

  @Override
  public String keyword() {
    return "check-accumulo-config";
  }

  @Override
  public String description() {
    return "Checks Accumulo configuration. Optionally provide the file path. Otherwise, will "
        + "attempt to find and check the config file";
  }

  @Override
  public void execute(String[] args) throws Exception {
    main(args);
  }
}
