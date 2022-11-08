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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.WithTestNames;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths not set by user input")
public class FileUtilTest extends WithTestNames {

  @TempDir
  private static File tempDir;
  private File accumuloDir;

  @BeforeEach
  public void createTmpDir() {
    accumuloDir = new File(tempDir, testName());
    assertTrue(accumuloDir.isDirectory() || accumuloDir.mkdir());
  }

  @Test
  public void testCleanupIndexOpWithDfsDir() throws IOException {
    // And a "unique" tmp directory for each volume
    File tmp1 = new File(accumuloDir, "tmp");
    assertTrue(tmp1.mkdirs() || tmp1.isDirectory());
    Path tmpPath1 = new Path(tmp1.toURI());

    HashMap<Property,String> testProps = new HashMap<>();
    testProps.put(Property.INSTANCE_VOLUMES, accumuloDir.getAbsolutePath());

    try (var fs = VolumeManagerImpl.getLocalForTesting(accumuloDir.getAbsolutePath())) {
      FileUtil.cleanupIndexOp(tmpPath1, fs, new ArrayList<>());
    }

    assertFalse(tmp1.exists(), "Expected " + tmp1 + " to be cleaned up but it wasn't");
  }

  @Test
  public void testCleanupIndexOpWithCommonParentVolume() throws IOException {
    File volumeDir = new File(accumuloDir, "volumes");
    assertTrue(volumeDir.mkdirs() || volumeDir.isDirectory());

    // Make some directories to simulate multiple volumes
    File v1 = new File(volumeDir, "v1"), v2 = new File(volumeDir, "v2");
    assertTrue(v1.mkdirs() || v1.isDirectory());
    assertTrue(v2.mkdirs() || v2.isDirectory());

    // And a "unique" tmp directory for each volume
    File tmp1 = new File(v1, "tmp"), tmp2 = new File(v2, "tmp");
    assertTrue(tmp1.mkdirs() || tmp1.isDirectory());
    assertTrue(tmp2.mkdirs() || tmp2.isDirectory());
    Path tmpPath1 = new Path(tmp1.toURI()), tmpPath2 = new Path(tmp2.toURI());

    HashMap<Property,String> testProps = new HashMap<>();
    testProps.put(Property.INSTANCE_VOLUMES, v1.toURI() + "," + v2.toURI());

    try (var fs = VolumeManagerImpl.getLocalForTesting(accumuloDir.getAbsolutePath())) {
      FileUtil.cleanupIndexOp(tmpPath1, fs, new ArrayList<>());
      assertFalse(tmp1.exists(), "Expected " + tmp1 + " to be cleaned up but it wasn't");
      FileUtil.cleanupIndexOp(tmpPath2, fs, new ArrayList<>());
      assertFalse(tmp2.exists(), "Expected " + tmp2 + " to be cleaned up but it wasn't");
    }
  }

  @Test
  public void testCleanupIndexOpWithCommonParentVolumeWithDepth() throws IOException {
    File volumeDir = new File(accumuloDir, "volumes");
    assertTrue(volumeDir.mkdirs() || volumeDir.isDirectory());

    // Make some directories to simulate multiple volumes
    File v1 = new File(volumeDir, "v1"), v2 = new File(volumeDir, "v2");
    assertTrue(v1.mkdirs() || v1.isDirectory());
    assertTrue(v2.mkdirs() || v2.isDirectory());

    // And a "unique" tmp directory for each volume
    // Make sure we can handle nested directories (a single tmpdir with potentially multiple unique
    // dirs)
    File tmp1 = new File(new File(v1, "tmp"), "tmp_1"),
        tmp2 = new File(new File(v2, "tmp"), "tmp_1");
    assertTrue(tmp1.mkdirs() || tmp1.isDirectory());
    assertTrue(tmp2.mkdirs() || tmp2.isDirectory());
    Path tmpPath1 = new Path(tmp1.toURI()), tmpPath2 = new Path(tmp2.toURI());

    HashMap<Property,String> testProps = new HashMap<>();
    testProps.put(Property.INSTANCE_VOLUMES, v1.toURI() + "," + v2.toURI());

    try (var fs = VolumeManagerImpl.getLocalForTesting(accumuloDir.getAbsolutePath())) {
      FileUtil.cleanupIndexOp(tmpPath1, fs, new ArrayList<>());
      assertFalse(tmp1.exists(), "Expected " + tmp1 + " to be cleaned up but it wasn't");
      FileUtil.cleanupIndexOp(tmpPath2, fs, new ArrayList<>());
      assertFalse(tmp2.exists(), "Expected " + tmp2 + " to be cleaned up but it wasn't");
    }
  }

  @Test
  public void testCleanupIndexOpWithoutCommonParentVolume() throws IOException {
    // Make some directories to simulate multiple volumes
    File v1 = new File(accumuloDir, "v1"), v2 = new File(accumuloDir, "v2");
    assertTrue(v1.mkdirs() || v1.isDirectory());
    assertTrue(v2.mkdirs() || v2.isDirectory());

    // And a "unique" tmp directory for each volume
    File tmp1 = new File(v1, "tmp"), tmp2 = new File(v2, "tmp");
    assertTrue(tmp1.mkdirs() || tmp1.isDirectory());
    assertTrue(tmp2.mkdirs() || tmp2.isDirectory());
    Path tmpPath1 = new Path(tmp1.toURI()), tmpPath2 = new Path(tmp2.toURI());

    HashMap<Property,String> testProps = new HashMap<>();
    testProps.put(Property.INSTANCE_VOLUMES, v1.toURI() + "," + v2.toURI());

    try (var fs = VolumeManagerImpl.getLocalForTesting(accumuloDir.getAbsolutePath())) {
      FileUtil.cleanupIndexOp(tmpPath1, fs, new ArrayList<>());
      assertFalse(tmp1.exists(), "Expected " + tmp1 + " to be cleaned up but it wasn't");
      FileUtil.cleanupIndexOp(tmpPath2, fs, new ArrayList<>());
      assertFalse(tmp2.exists(), "Expected " + tmp2 + " to be cleaned up but it wasn't");
    }
  }

  @Test
  public void testCleanupIndexOpWithoutCommonParentVolumeWithDepth() throws IOException {
    // Make some directories to simulate multiple volumes
    File v1 = new File(accumuloDir, "v1"), v2 = new File(accumuloDir, "v2");
    assertTrue(v1.mkdirs() || v1.isDirectory());
    assertTrue(v2.mkdirs() || v2.isDirectory());

    // And a "unique" tmp directory for each volume
    // Make sure we can handle nested directories (a single tmpdir with potentially multiple unique
    // dirs)
    File tmp1 = new File(new File(v1, "tmp"), "tmp_1"),
        tmp2 = new File(new File(v2, "tmp"), "tmp_1");
    assertTrue(tmp1.mkdirs() || tmp1.isDirectory());
    assertTrue(tmp2.mkdirs() || tmp2.isDirectory());
    Path tmpPath1 = new Path(tmp1.toURI()), tmpPath2 = new Path(tmp2.toURI());

    HashMap<Property,String> testProps = new HashMap<>();
    testProps.put(Property.INSTANCE_VOLUMES, v1.toURI() + "," + v2.toURI());

    try (var fs = VolumeManagerImpl.getLocalForTesting(accumuloDir.getAbsolutePath())) {
      FileUtil.cleanupIndexOp(tmpPath1, fs, new ArrayList<>());
      assertFalse(tmp1.exists(), "Expected " + tmp1 + " to be cleaned up but it wasn't");
      FileUtil.cleanupIndexOp(tmpPath2, fs, new ArrayList<>());
      assertFalse(tmp2.exists(), "Expected " + tmp2 + " to be cleaned up but it wasn't");
    }
  }
}
