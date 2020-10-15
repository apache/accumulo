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
package org.apache.accumulo.core.volume;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.slf4j.LoggerFactory;

public class VolumeImplTest {

  @Test
  public void testFileSystemInequivalence() {
    Configuration hadoopConf = createMock(Configuration.class);
    FileSystem fs = createMock(FileSystem.class), other = createMock(FileSystem.class);

    String basePath = "/accumulo";

    expect(fs.getConf()).andReturn(hadoopConf).anyTimes();
    expect(fs.getUri()).andReturn(URI.create("hdfs://localhost:8020")).anyTimes();
    expect(other.getUri()).andReturn(URI.create("hdfs://otherhost:8020")).anyTimes();

    replay(fs, other);

    VolumeImpl volume = new VolumeImpl(fs, basePath);

    assertFalse(volume.equivalentFileSystems(other));

    verify(fs, other);
  }

  @Test
  public void testFileSystemEquivalence() {
    Configuration hadoopConf = createMock(Configuration.class);
    FileSystem fs = createMock(FileSystem.class), other = createMock(FileSystem.class);
    String basePath = "/accumulo";

    expect(fs.getConf()).andReturn(hadoopConf).anyTimes();
    expect(fs.getUri()).andReturn(URI.create("hdfs://myhost:8020/")).anyTimes();
    expect(other.getUri()).andReturn(URI.create("hdfs://myhost:8020")).anyTimes();

    replay(fs, other);

    VolumeImpl volume = new VolumeImpl(fs, basePath);

    assertTrue(volume.equivalentFileSystems(other));

    verify(fs, other);
  }

  @Test
  public void testBasePathInequivalence() {
    FileSystem fs = createMock(FileSystem.class);

    VolumeImpl volume = new VolumeImpl(fs, "/accumulo");

    assertFalse(volume.isAncestorPathOf(new Path("/something/accumulo")));
    assertFalse(volume.isAncestorPathOf(new Path("/accumulo2")));
    assertFalse(volume.isAncestorPathOf(new Path("/accumulo/..")));
  }

  @Test
  public void testBasePathEquivalence() {
    FileSystem fs = createMock(FileSystem.class);

    final String basePath = "/accumulo";
    VolumeImpl volume = new VolumeImpl(fs, basePath);

    // Bare path should match
    assertTrue(volume.isAncestorPathOf(new Path(basePath)));
    // Prefix should also match
    assertTrue(volume.isAncestorPathOf(new Path(basePath + "/tables/1/F000001.rf")));
  }

  @Test
  public void testPrefixChild() throws IOException {
    FileSystem fs = new Path("file:///").getFileSystem(new Configuration(false));
    Volume volume = new VolumeImpl(fs, "/tmp/accumulo/");
    assertEquals("file:/tmp/accumulo/abc", volume.prefixChild("abc").toString());
    assertEquals("file:/tmp/accumulo/abc/def", volume.prefixChild(" abc/def/ ").toString());
    assertEquals("file:/tmp/accumulo/ghi", volume.prefixChild(" ghi/// ").toString());
    assertEquals("file:/tmp/accumulo", volume.prefixChild(" .").toString());
    assertEquals("file:/tmp/accumulo/abc", volume.prefixChild("./abc/.").toString());
    assertEquals("file:/tmp/accumulo/abc/def", volume.prefixChild("abc/./def/").toString());
    assertEquals("file:/tmp/accumulo/abc", volume.prefixChild("./abc").toString());
    Set.of("/abc", "./abc/..", "abc/../def/", "../abc", " .. ", "file:/abc", "hdfs://host:1234")
        .forEach(s -> {
          assertThrows(IllegalArgumentException.class, () -> {
            volume.prefixChild(s);
            LoggerFactory.getLogger(VolumeImplTest.class).error("Should have thrown on " + s);
          });
        });
    FileSystem fs2 = new Path("hdfs://127.0.0.1:1234/").getFileSystem(new Configuration(false));
    var volume2 = new VolumeImpl(fs2, "/tmp/accumulo/");
    assertEquals("hdfs://127.0.0.1:1234/tmp/accumulo/abc", volume2.prefixChild("abc").toString());
  }

  @Test
  public void testContains() throws IOException {
    FileSystem fs = new Path("file:///").getFileSystem(new Configuration(false));
    var volume = new VolumeImpl(fs, "/tmp/accumulo/");
    Set.of("abc", " abc/def/ ", " ghi/// ").forEach(s -> {
      assertTrue(volume.containsPath(volume.prefixChild(s)));
    });
  }

}
