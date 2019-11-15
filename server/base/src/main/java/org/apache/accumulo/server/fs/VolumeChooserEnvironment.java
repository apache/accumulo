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

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;

public interface VolumeChooserEnvironment {

  /**
   * A scope the volume chooser environment; a TABLE scope should be accompanied by a tableId.
   *
   * @since 2.0.0
   */
  public static enum ChooserScope {
    DEFAULT, TABLE, INIT, LOGGER
  }

  /**
   * The end row of the tablet for which a volume is being chosen. Only call this when the scope is
   * TABLE
   *
   * @since 2.0.0
   */
  public Text getEndRow();

  public boolean hasTableId();

  public TableId getTableId();

  /**
   * @since 2.0.0
   */
  public ChooserScope getScope();

  /**
   * @since 2.0.0
   */
  public ServiceEnvironment getServiceEnv();

  /**
   * @since 2.0.0
   */
  public FileSystem getFileSystem(String option);
}
