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
package org.apache.accumulo.test.upgrade;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.miniclusterImpl.ProcessNotFoundException;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpgradeTestUtils {

  public static final String BACKUP_DIR = "/target/upgrade-tests/backup/";
  public static final String WORK_DIR = "/target/upgrade-tests/work/";
  public static final String ROOT_PASSWORD = "979d55ae-98fd-4d22-9b1c-2d5723546a5e";

  public static final Logger log = LoggerFactory.getLogger(UpgradeTestUtils.class);

  public static File getTestDir(String version, String testName) {
    return new File(System.getProperty("user.dir") + WORK_DIR + testName + "/" + version);
  }

  public static File getBackupDir(String version, String testName) {
    return new File(System.getProperty("user.dir") + BACKUP_DIR + testName + "/" + version);
  }

  /**
   * For a given test name finds all the versions of all dirs created using
   * {@link #getTestDir(String, String)}
   */
  public static List<String> findVersions(String testName) {
    var testRoot = new File(System.getProperty("user.dir") + WORK_DIR + testName);
    var files = testRoot.listFiles();
    if (files == null) {
      return List.of();
    }
    return Arrays.stream(files).filter(File::isDirectory).map(File::getName)
        .collect(Collectors.toList());
  }

  public static void deleteTest(String version, String testName) {
    FileUtils.deleteQuietly(getTestDir(version, testName));
    FileUtils.deleteQuietly(getBackupDir(version, testName));
  }

  /**
   * This method facilitates running upgrade test multiple times. Upgrade will change persisted data
   * making it impossible to run an upgrade test a second time. Using this method an upgrade test
   * can backup and restore persisted data before running a test.
   */
  public static void backupOrRestore(String version, String testName) throws IOException {

    File testDir = getTestDir(version, testName);
    File backupDir = getBackupDir(version, testName);

    if (backupDir.exists()) {
      log.info("Restoring backup {} -> {}", backupDir, testDir);
      FileUtils.deleteQuietly(testDir);
      FileUtils.copyDirectory(backupDir, testDir);
    } else {
      log.info("Creating backup {} -> {}", testDir, backupDir);
      FileUtils.copyDirectory(testDir, backupDir);
    }
  }

  public static void killAll(MiniAccumuloClusterImpl cluster) {
    cluster.getProcesses().forEach((server, processes) -> {
      processes.forEach(process -> {
        try {
          cluster.killProcess(server, process);
        } catch (ProcessNotFoundException | InterruptedException e) {
          throw new IllegalStateException(e);
        }
      });
    });
  }

}
