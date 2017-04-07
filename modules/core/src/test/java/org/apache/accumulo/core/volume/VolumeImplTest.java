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
package org.apache.accumulo.core.volume;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class VolumeImplTest {

  @Test
  public void testFileSystemInequivalence() {
    FileSystem fs = createMock(FileSystem.class), other = createMock(FileSystem.class);
    String basePath = "/accumulo";

    VolumeImpl volume = new VolumeImpl(fs, basePath);

    expect(fs.getUri()).andReturn(URI.create("hdfs://localhost:8020")).anyTimes();
    expect(other.getUri()).andReturn(URI.create("hdfs://otherhost:8020")).anyTimes();

    replay(fs, other);

    assertFalse(volume.equivalentFileSystems(other));

    verify(fs, other);
  }

  @Test
  public void testFileSystemEquivalence() {
    FileSystem fs = createMock(FileSystem.class), other = createMock(FileSystem.class);
    String basePath = "/accumulo";

    VolumeImpl volume = new VolumeImpl(fs, basePath);

    expect(fs.getUri()).andReturn(URI.create("hdfs://myhost:8020")).anyTimes();
    expect(other.getUri()).andReturn(URI.create("hdfs://myhost:8020")).anyTimes();

    replay(fs, other);

    assertTrue(volume.equivalentFileSystems(other));

    verify(fs, other);
  }

  @Test
  public void testBasePathInequivalence() {
    FileSystem fs = createMock(FileSystem.class);

    VolumeImpl volume = new VolumeImpl(fs, "/accumulo");

    assertFalse(volume.equivalentPaths(new Path("/something/accumulo")));
  }

  @Test
  public void testBasePathEquivalence() {
    FileSystem fs = createMock(FileSystem.class);

    final String basePath = "/accumulo";
    VolumeImpl volume = new VolumeImpl(fs, basePath);

    // Bare path should match
    assertTrue(volume.equivalentPaths(new Path(basePath)));
    // Prefix should also match
    assertTrue(volume.equivalentPaths(new Path(basePath + "/tables/1/F000001.rf")));
  }

}
