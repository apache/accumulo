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
package org.apache.accumulo.tserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.server.fs.FileRef;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 *
 */
public class RootFiles {

  private static Logger log = Logger.getLogger(RootFiles.class);

  static void prepareReplacement(VolumeManager fs, Path location, Set<FileRef> oldDatafiles, String compactName) throws IOException {
    for (FileRef ref : oldDatafiles) {
      Path path = ref.path();
      Tablet.rename(fs, path, new Path(location + "/delete+" + compactName + "+" + path.getName()));
    }
  }

  static void renameReplacement(VolumeManager fs, FileRef tmpDatafile, FileRef newDatafile) throws IOException {
    if (fs.exists(newDatafile.path())) {
      log.error("Target map file already exist " + newDatafile, new Exception());
      throw new IllegalStateException("Target map file already exist " + newDatafile);
    }

    Tablet.rename(fs, tmpDatafile.path(), newDatafile.path());
  }

  static void finishReplacement(AccumuloConfiguration acuTableConf, VolumeManager fs, Path location, Set<FileRef> oldDatafiles, String compactName)
      throws IOException {
    // start deleting files, if we do not finish they will be cleaned
    // up later
    for (FileRef ref : oldDatafiles) {
      Path path = ref.path();
      Path deleteFile = new Path(location + "/delete+" + compactName + "+" + path.getName());
      if (acuTableConf.getBoolean(Property.GC_TRASH_IGNORE) || !fs.moveToTrash(deleteFile))
        fs.deleteRecursively(deleteFile);
    }
  }

  public static void replaceFiles(AccumuloConfiguration acuTableConf, VolumeManager fs, Path location, Set<FileRef> oldDatafiles, FileRef tmpDatafile,
      FileRef newDatafile) throws IOException {
    String compactName = newDatafile.path().getName();

    prepareReplacement(fs, location, oldDatafiles, compactName);
    renameReplacement(fs, tmpDatafile, newDatafile);
    finishReplacement(acuTableConf, fs, location, oldDatafiles, compactName);
  }

  public static Collection<String> cleanupReplacement(VolumeManager fs, FileStatus[] files, boolean deleteTmp) throws IOException {
    /*
     * called in constructor and before major compactions
     */
    Collection<String> goodFiles = new ArrayList<String>(files.length);

    for (FileStatus file : files) {

      String path = file.getPath().toString();
      if (file.getPath().toUri().getScheme() == null) {
        // depending on the behavior of HDFS, if list status does not return fully qualified volumes then could switch to the default volume
        throw new IllegalArgumentException("Require fully qualified paths " + file.getPath());
      }

      String filename = file.getPath().getName();

      // check for incomplete major compaction, this should only occur
      // for root tablet
      if (filename.startsWith("delete+")) {
        String expectedCompactedFile = path.substring(0, path.lastIndexOf("/delete+")) + "/" + filename.split("\\+")[1];
        if (fs.exists(new Path(expectedCompactedFile))) {
          // compaction finished, but did not finish deleting compacted files.. so delete it
          if (!fs.deleteRecursively(file.getPath()))
            log.warn("Delete of file: " + file.getPath().toString() + " return false");
          continue;
        }
        // compaction did not finish, so put files back

        // reset path and filename for rest of loop
        filename = filename.split("\\+", 3)[2];
        path = path.substring(0, path.lastIndexOf("/delete+")) + "/" + filename;

        Tablet.rename(fs, file.getPath(), new Path(path));
      }

      if (filename.endsWith("_tmp")) {
        if (deleteTmp) {
          log.warn("cleaning up old tmp file: " + path);
          if (!fs.deleteRecursively(file.getPath()))
            log.warn("Delete of tmp file: " + file.getPath().toString() + " return false");

        }
        continue;
      }

      if (!filename.startsWith(Constants.MAPFILE_EXTENSION + "_") && !FileOperations.getValidExtensions().contains(filename.split("\\.")[1])) {
        log.error("unknown file in tablet: " + path);
        continue;
      }

      goodFiles.add(path);
    }

    return goodFiles;
  }
}
