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

package org.apache.accumulo.tserver.log;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.log.SortedLogState;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestUpgradePathForWALogs {

  private static final String WALOG_FROM_15 = "/walog-from-15.walog";
  private static final String WALOG_FROM_16 = "/walog-from-16.walog";
  private static File testDir;

  VolumeManager fs;

  @BeforeClass
  public static void createTestDirectory() {
    File baseDir = new File(System.getProperty("user.dir") + "/target/upgrade-tests");
    assertTrue(baseDir.mkdirs() || baseDir.isDirectory());
    testDir = new File(baseDir, TestUpgradePathForWALogs.class.getName());
    FileUtils.deleteQuietly(testDir);
    assertTrue(testDir.mkdir() || testDir.isDirectory());
  }

  @Rule
  public TemporaryFolder root = new TemporaryFolder(testDir);

  @Before
  public void setUp() throws Exception {
    // quiet log messages about compress.CodecPool
    Logger.getRootLogger().setLevel(Level.ERROR);
    root.create();
    String path = root.getRoot().getAbsolutePath() + "/manyMaps";
    fs = VolumeManagerImpl.getLocal(path);
    Path manyMapsPath = new Path("file://" + path);
    fs.mkdirs(manyMapsPath);
    fs.create(SortedLogState.getFinishedMarkerPath(manyMapsPath)).close();
  }

  @Test
  public void testUpgradeOf15WALog() throws IOException {
    InputStream walogStream = null;
    OutputStream walogInHDFStream = null;

    try {

      walogStream = getClass().getResourceAsStream(WALOG_FROM_15);
      walogInHDFStream = new FileOutputStream(new File(root.getRoot().getAbsolutePath() + WALOG_FROM_15));

      IOUtils.copyLarge(walogStream, walogInHDFStream);
      walogInHDFStream.flush();
      walogInHDFStream.close();
      walogInHDFStream = null;

      LogSorter logSorter = new LogSorter(null, fs, SiteConfiguration.getInstance());
      LogSorter.LogProcessor logProcessor = logSorter.new LogProcessor();

      logProcessor.sort(WALOG_FROM_15, new Path("file://" + root.getRoot().getAbsolutePath() + WALOG_FROM_15), "file://" + root.getRoot().getAbsolutePath()
          + "/manyMaps");

    } finally {
      if (walogStream != null) {
        walogStream.close();
      }

      if (walogInHDFStream != null) {
        walogInHDFStream.close();
      }
    }
  }

  @Test
  public void testBasic16WALogRead() throws IOException {
    String walogToTest = WALOG_FROM_16;

    InputStream walogStream = null;
    OutputStream walogInHDFStream = null;

    try {

      walogStream = getClass().getResourceAsStream(walogToTest);
      walogInHDFStream = new FileOutputStream(new File(root.getRoot().getAbsolutePath() + walogToTest));

      IOUtils.copyLarge(walogStream, walogInHDFStream);
      walogInHDFStream.flush();
      walogInHDFStream.close();
      walogInHDFStream = null;

      LogSorter logSorter = new LogSorter(null, fs, SiteConfiguration.getInstance());
      LogSorter.LogProcessor logProcessor = logSorter.new LogProcessor();

      logProcessor.sort(walogToTest, new Path("file://" + root.getRoot().getAbsolutePath() + walogToTest), "file://" + root.getRoot().getAbsolutePath()
          + "/manyMaps");

    } finally {
      if (walogStream != null) {
        walogStream.close();
      }

      if (walogInHDFStream != null) {
        walogInHDFStream.close();
      }
    }
  }

}
