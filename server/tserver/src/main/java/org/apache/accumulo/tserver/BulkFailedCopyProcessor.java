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
package org.apache.accumulo.tserver;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;

import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.zookeeper.DistributedWorkQueue.Processor;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copy failed bulk imports.
 */
public class BulkFailedCopyProcessor implements Processor {

  private static final Logger log = LoggerFactory.getLogger(BulkFailedCopyProcessor.class);

  private ServerContext context;

  BulkFailedCopyProcessor(ServerContext context) {
    this.context = context;
  }

  @Override
  public Processor newProcessor() {
    return new BulkFailedCopyProcessor(context);
  }

  @Override
  public void process(String workID, byte[] data) {

    String[] paths = new String(data, UTF_8).split(",");

    Path orig = new Path(paths[0]);
    Path dest = new Path(paths[1]);
    Path tmp = new Path(dest.getParent(), dest.getName() + ".tmp");

    VolumeManager vm = context.getVolumeManager();
    try {
      FileSystem origFs = vm.getFileSystemByPath(orig);
      FileSystem destFs = vm.getFileSystemByPath(dest);

      FileUtil.copy(origFs, orig, destFs, tmp, false, true, context.getHadoopConf());
      destFs.rename(tmp, dest);
      log.debug("copied {} to {}", orig, dest);
    } catch (IOException ex) {
      try {
        FileSystem destFs = vm.getFileSystemByPath(dest);
        destFs.create(dest).close();
        log.warn(" marked " + dest + " failed", ex);
      } catch (IOException e) {
        log.error("Unable to create failure flag file " + dest, e);
      }
    }

  }

}
