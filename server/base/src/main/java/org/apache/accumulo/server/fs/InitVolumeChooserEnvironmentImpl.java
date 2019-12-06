/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.fs;

import java.io.IOException;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.server.ServiceEnvironmentImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

public class InitVolumeChooserEnvironmentImpl implements VolumeChooserEnvironment {

  private final AccumuloConfiguration conf;
  private final VolumeManager volumeManager;

  public InitVolumeChooserEnvironmentImpl(AccumuloConfiguration acfg) {
    this.conf = acfg;
    try {
      volumeManager = VolumeManagerImpl.get(acfg, new Configuration());
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public Text getEndRow() {
    throw new UnsupportedOperationException("not supported for init");
  }

  @Override
  public boolean hasTableId() {
    throw new UnsupportedOperationException("not supported for init");
  }

  @Override
  public TableId getTableId() {
    throw new UnsupportedOperationException("not supported for init");
  }

  @Override
  public ChooserScope getScope() {
    return ChooserScope.INIT;
  }

  @Override
  public ServiceEnvironment getServiceEnv() {
    return new ServiceEnvironmentImpl(null, conf);
  }

  @Override
  public FileSystem getFileSystem(String option) {
    return volumeManager.getVolumeByPath(new Path(option)).getFileSystem();
  }
}
