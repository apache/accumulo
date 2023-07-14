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
package org.apache.accumulo.server.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileUtil {

  public static class FileInfo {
    private final Text firstKey;
    private final Text lastKey;

    public FileInfo(Key firstKey, Key lastKey) {
      this.firstKey = firstKey.getRow();
      this.lastKey = lastKey.getRow();
    }

    public Text getFirstRow() {
      return firstKey;
    }

    public Text getLastRow() {
      return lastKey;
    }
  }

  private static final Logger log = LoggerFactory.getLogger(FileUtil.class);

  // ELASTICITY_TODO this is only used by test. Determine what the test are doing and if some
  // functionality is missing in the new split code.
  protected static void cleanupIndexOp(Path tmpDir, VolumeManager fs,
      ArrayList<FileSKVIterator> readers) throws IOException {
    // close all of the index sequence files
    for (FileSKVIterator r : readers) {
      try {
        if (r != null) {
          r.close();
        }
      } catch (IOException e) {
        // okay, try to close the rest anyway
        log.error("{}", e.getMessage(), e);
      }
    }

    if (tmpDir != null) {
      FileSystem actualFs = fs.getFileSystemByPath(tmpDir);
      if (actualFs.exists(tmpDir)) {
        fs.deleteRecursively(tmpDir);
        return;
      }

      log.error("Did not delete tmp dir because it wasn't a tmp dir {}", tmpDir);
    }
  }

  public static <T extends TabletFile> Map<T,FileInfo> tryToGetFirstAndLastRows(
      ServerContext context, TableConfiguration tableConf, Set<T> dataFiles) {

    HashMap<T,FileInfo> dataFilesInfo = new HashMap<>();

    long t1 = System.currentTimeMillis();

    for (T dataFile : dataFiles) {

      FileSKVIterator reader = null;
      FileSystem ns = context.getVolumeManager().getFileSystemByPath(dataFile.getPath());
      try {
        reader = FileOperations.getInstance().newReaderBuilder()
            .forFile(dataFile, ns, ns.getConf(), tableConf.getCryptoService())
            .withTableConfiguration(tableConf).build();

        Key firstKey = reader.getFirstKey();
        if (firstKey != null) {
          dataFilesInfo.put(dataFile, new FileInfo(firstKey, reader.getLastKey()));
        }

      } catch (IOException ioe) {
        log.warn("Failed to read data file to determine first and last key : " + dataFile, ioe);
      } finally {
        if (reader != null) {
          try {
            reader.close();
          } catch (IOException ioe) {
            log.warn("failed to close " + dataFile, ioe);
          }
        }
      }

    }

    long t2 = System.currentTimeMillis();

    String message = String.format("Found first and last keys for %d data files in %6.2f secs",
        dataFiles.size(), (t2 - t1) / 1000.0);
    if (t2 - t1 > 500) {
      log.debug(message);
    } else {
      log.trace(message);
    }

    return dataFilesInfo;
  }
}
