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
package org.apache.accumulo.gc;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileJanitor {

  private static final Logger LOG = LoggerFactory.getLogger(FileJanitor.class);

  enum SendFilesToTrash {

    TRUE("true"), FALSE("false"), IMPORTS_ONLY("bulk_imports_only");

    final String val;

    SendFilesToTrash(String val) {
      this.val = val;
    }

    public static SendFilesToTrash fromBoolean(boolean b) {
      if (b) {
        return TRUE;
      }
      return FALSE;
    }

    public static SendFilesToTrash fromString(String val) {
      switch (val.toLowerCase()) {
        case "true":
          return TRUE;
        case "false":
          return FALSE;
        case "bulk_imports_only":
          return IMPORTS_ONLY;
        default:
          throw new IllegalArgumentException("Unknown value: " + val);
      }
    }
  }

  private final ServerContext context;
  private final SendFilesToTrash usingTrash;

  @SuppressWarnings("removal")
  public FileJanitor(ServerContext context) {
    this.context = context;
    final AccumuloConfiguration conf = this.context.getConfiguration();
    if (conf.isPropertySet(Property.GC_TRASH_IGNORE) && conf.isPropertySet(Property.GC_USE_TRASH)) {
      throw new IllegalStateException("Cannot specify both " + Property.GC_TRASH_IGNORE.getKey()
          + " and " + Property.GC_USE_TRASH.getKey() + " properties.");
    }
    if (conf.isPropertySet(Property.GC_TRASH_IGNORE)) {
      this.usingTrash = SendFilesToTrash.fromBoolean(!conf.getBoolean(Property.GC_TRASH_IGNORE));
    } else if (conf.isPropertySet(Property.GC_USE_TRASH)) {
      this.usingTrash = SendFilesToTrash.fromString(conf.get(Property.GC_USE_TRASH));
    } else {
      LOG.warn(
          "Neither GC trash property was set ({} or {}). Defaulting to using trash for all files (if available).",
          Property.GC_TRASH_IGNORE.getKey(), Property.GC_USE_TRASH.getKey());
      this.usingTrash = SendFilesToTrash.TRUE;
    }
  }

  public ServerContext getContext() {
    return this.context;
  }

  /**
   * Moves a file to trash. If this garbage collector is not using trash, this method returns false
   * and leaves the file alone. If the file is missing, this method returns false as opposed to
   * throwing an exception.
   *
   * @return true if the file was moved to trash
   * @throws IOException if the volume manager encountered a problem
   */
  public boolean moveToTrash(Path path) throws IOException {
    final VolumeManager fs = getContext().getVolumeManager();

    if (this.isUsingTrash() == SendFilesToTrash.FALSE) {
      return false;
    } else if (this.isUsingTrash() == SendFilesToTrash.IMPORTS_ONLY
        && !path.getName().startsWith("I")) {
      return false;
    } else {
      try {
        return fs.moveToTrash(path);
      } catch (FileNotFoundException ex) {
        return false;
      }
    }
  }

  /**
   * Checks if the volume manager should move files to the trash rather than delete them.
   *
   * @return true if trash is used
   */
  public SendFilesToTrash isUsingTrash() {
    return this.usingTrash;
  }

}
