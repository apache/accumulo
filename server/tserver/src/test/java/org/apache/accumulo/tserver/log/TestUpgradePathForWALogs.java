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
package org.apache.accumulo.tserver.log;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.log.SortedLogState;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths not set by user input")
public class TestUpgradePathForWALogs {

  private static final String WALOG_FROM_15 = "/walog-from-15.walog";
  private static final String WALOG_FROM_16 = "/walog-from-16.walog";

  AccumuloConfiguration config = DefaultConfiguration.getInstance();
  VolumeManager fs;

  @Rule
  public TemporaryFolder tempFolder =
      new TemporaryFolder(new File(System.getProperty("user.dir"), "target"));

  @Before
  public void setUp() throws Exception {
    File workDir = tempFolder.newFolder();
    String path = workDir.getAbsolutePath();
    assertTrue(workDir.delete());
    fs = VolumeManagerImpl.getLocalForTesting(path);
    Path manyMapsPath = new Path("file://" + path);
    fs.mkdirs(manyMapsPath);
    fs.create(SortedLogState.getFinishedMarkerPath(manyMapsPath)).close();
  }

  @After
  public void tearDown() throws IOException {
    fs.close();
  }

  @Test
  public void testUpgradeOf15WALog() throws IOException {
    InputStream walogStream = null;
    OutputStream walogInHDFStream = null;

    try {

      walogStream = getClass().getResourceAsStream(WALOG_FROM_15);
      walogInHDFStream =
          new FileOutputStream(new File(tempFolder.getRoot().getAbsolutePath() + WALOG_FROM_15));

      IOUtils.copyLarge(walogStream, walogInHDFStream);
      walogInHDFStream.flush();
      walogInHDFStream.close();
      walogInHDFStream = null;

      LogSorter logSorter = new LogSorter(null, fs, config);
      LogSorter.LogProcessor logProcessor = logSorter.new LogProcessor();

      logProcessor.sort(WALOG_FROM_15,
          new Path("file://" + tempFolder.getRoot().getAbsolutePath() + WALOG_FROM_15),
          "file://" + tempFolder.getRoot().getAbsolutePath() + "/manyMaps");

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
      walogInHDFStream =
          new FileOutputStream(new File(tempFolder.getRoot().getAbsolutePath() + walogToTest));

      IOUtils.copyLarge(walogStream, walogInHDFStream);
      walogInHDFStream.flush();
      walogInHDFStream.close();
      walogInHDFStream = null;

      LogSorter logSorter = new LogSorter(null, fs, config);
      LogSorter.LogProcessor logProcessor = logSorter.new LogProcessor();

      logProcessor.sort(walogToTest,
          new Path("file://" + tempFolder.getRoot().getAbsolutePath() + walogToTest),
          "file://" + tempFolder.getRoot().getAbsolutePath() + "/manyMaps");

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
