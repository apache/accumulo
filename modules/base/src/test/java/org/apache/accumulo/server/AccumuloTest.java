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
package org.apache.accumulo.server;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

public class AccumuloTest {
  private FileSystem fs;
  private Path path;

  @Before
  public void setUp() throws Exception {
    fs = createMock(FileSystem.class);
    path = createMock(Path.class);
  }

  private FileStatus[] mockPersistentVersion(String s) {
    FileStatus[] files = new FileStatus[1];
    files[0] = createMock(FileStatus.class);
    Path filePath = createMock(Path.class);
    expect(filePath.getName()).andReturn(s);
    replay(filePath);
    expect(files[0].getPath()).andReturn(filePath);
    replay(files[0]);
    return files;
  }

  @Test
  public void testGetAccumuloPersistentVersion() throws Exception {
    FileStatus[] files = mockPersistentVersion("42");
    expect(fs.listStatus(path)).andReturn(files);
    replay(fs);

    assertEquals(42, Accumulo.getAccumuloPersistentVersion(fs, path));
  }

  @Test
  public void testGetAccumuloPersistentVersion_Null() throws Exception {
    expect(fs.listStatus(path)).andReturn(null);
    replay(fs);

    assertEquals(-1, Accumulo.getAccumuloPersistentVersion(fs, path));
  }

  @Test
  public void testGetAccumuloPersistentVersion_Empty() throws Exception {
    expect(fs.listStatus(path)).andReturn(new FileStatus[0]);
    replay(fs);

    assertEquals(-1, Accumulo.getAccumuloPersistentVersion(fs, path));
  }

  @Test(expected = RuntimeException.class)
  public void testGetAccumuloPersistentVersion_Fail() throws Exception {
    expect(fs.listStatus(path)).andThrow(new FileNotFoundException());
    replay(fs);

    assertEquals(-1, Accumulo.getAccumuloPersistentVersion(fs, path));
  }

  @Test
  public void testLocateLogConfig() throws Exception {
    File confDir = new File(FileUtils.getTempDirectory(), "AccumuloTest" + System.currentTimeMillis());
    String confDirName = confDir.getAbsolutePath();
    assertTrue("Failed to make test configuration directory", confDir.mkdir());
    try {
      File appProps = new File(confDir, "flogger_logger.properties");
      String appPropsName = String.format("%s/flogger_logger.properties", confDirName);
      FileUtils.touch(appProps);
      File genericXml = new File(confDir, "generic_logger.xml");
      String genericXmlName = String.format("%s/generic_logger.xml", confDirName);
      FileUtils.touch(genericXml);

      assertEquals(appPropsName, Accumulo.locateLogConfig(confDirName, "flogger"));
      assertEquals(genericXmlName, Accumulo.locateLogConfig(confDirName, "flagger"));

      assertTrue("Failed to delete log configuration file", appProps.delete());
      assertEquals(genericXmlName, Accumulo.locateLogConfig(confDirName, "flogger"));
    } finally {
      FileUtils.deleteDirectory(confDir);
    }
  }

  @Test
  public void testLocateLogConfig_Default() throws Exception {
    File confDir = new File(FileUtils.getTempDirectory(), "AccumuloTest" + System.currentTimeMillis());
    String confDirName = confDir.getAbsolutePath();
    assertTrue("Failed to make test configuration directory", confDir.mkdir());
    try {
      String genericXmlName = String.format("%s/generic_logger.xml", confDirName);

      assertEquals(genericXmlName, Accumulo.locateLogConfig(confDirName, "flogger"));
    } finally {
      FileUtils.deleteDirectory(confDir);
    }
  }

  @Test
  public void testLocateLogConfig_Explicit() throws Exception {
    File confDir = new File(FileUtils.getTempDirectory(), "AccumuloTest" + System.currentTimeMillis());
    String confDirName = confDir.getAbsolutePath();
    System.setProperty("log4j.configuration", "myconfig.xml");
    try {
      assertEquals("myconfig.xml", Accumulo.locateLogConfig(confDirName, "flogger"));
    } finally {
      FileUtils.deleteDirectory(confDir);
      System.clearProperty("log4j.configuration");
    }
  }
}
