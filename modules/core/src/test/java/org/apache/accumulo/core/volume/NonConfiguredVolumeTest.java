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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class NonConfiguredVolumeTest {

  private NonConfiguredVolume volume;

  @Before
  public void create() throws IOException {
    volume = new NonConfiguredVolume(FileSystem.getLocal(new Configuration()));
  }

  @Test
  public void testSameFileSystem() throws IOException {
    Assert.assertEquals(FileSystem.getLocal(new Configuration()), volume.getFileSystem());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGetBasePathFails() {
    volume.getBasePath();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testPrefixChildPath() {
    volume.prefixChild(new Path("/foo"));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testPrefixChildString() {
    volume.prefixChild("/foo");
  }

  @Test
  public void testEquality() throws IOException {
    Volume newVolume = new NonConfiguredVolume(FileSystem.getLocal(new Configuration()));
    Assert.assertEquals(volume, newVolume);
  }

  @Test
  public void testHashCode() throws IOException {
    Volume newVolume = new NonConfiguredVolume(FileSystem.getLocal(new Configuration()));
    Assert.assertEquals(volume.hashCode(), newVolume.hashCode());
  }
}
