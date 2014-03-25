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
package org.apache.accumulo.server.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.io.Files;

/**
 * 
 */
public class FileUtilTest {

  @SuppressWarnings("deprecation")
  @Test
  public void testCleanupIndexOpWithDfsDir() throws IOException {
    File dfsDir = Files.createTempDir();
    
    try {
      // And a "unique" tmp directory for each volume
      File tmp1 = new File(dfsDir, "tmp");
      tmp1.mkdirs();
      Path tmpPath1 = new Path(tmp1.toURI());
      
      HashMap<Property,String> testProps = new HashMap<Property,String>();
      testProps.put(Property.INSTANCE_DFS_DIR, dfsDir.getAbsolutePath());
      
      AccumuloConfiguration testConf = new FileUtilTestConfiguration(testProps);
      VolumeManager fs = VolumeManagerImpl.getLocal(dfsDir.getAbsolutePath());
      
      FileUtil.cleanupIndexOp(testConf, tmpPath1, fs, new ArrayList<FileSKVIterator>());
      
      Assert.assertFalse("Expected " + tmp1 + " to be cleaned up but it wasn't", tmp1.exists());
    } finally {
      FileUtils.deleteQuietly(dfsDir);
    }
  }

  @Test
  public void testCleanupIndexOpWithCommonParentVolume() throws IOException {
    File accumuloDir = Files.createTempDir();
    
    try {
      File volumeDir = new File(accumuloDir, "volumes");
      volumeDir.mkdirs();

      // Make some directories to simulate multiple volumes
      File v1 = new File(volumeDir, "v1"), v2 = new File(volumeDir, "v2");
      v1.mkdirs();
      v2.mkdirs();

      // And a "unique" tmp directory for each volume
      File tmp1 = new File(v1, "tmp"), tmp2 = new File(v2, "tmp");
      tmp1.mkdirs();
      tmp2.mkdirs();
      Path tmpPath1 = new Path(tmp1.toURI()), tmpPath2 = new Path(tmp2.toURI());
      
      HashMap<Property,String> testProps = new HashMap<Property,String>();
      testProps.put(Property.INSTANCE_VOLUMES, v1.toURI().toString() + "," + v2.toURI().toString());
      
      AccumuloConfiguration testConf = new FileUtilTestConfiguration(testProps);
      VolumeManager fs = VolumeManagerImpl.getLocal(accumuloDir.getAbsolutePath());
      
      FileUtil.cleanupIndexOp(testConf, tmpPath1, fs, new ArrayList<FileSKVIterator>());
      
      Assert.assertFalse("Expected " + tmp1 + " to be cleaned up but it wasn't", tmp1.exists());
      
      FileUtil.cleanupIndexOp(testConf, tmpPath2, fs, new ArrayList<FileSKVIterator>());
      
      Assert.assertFalse("Expected " + tmp2 + " to be cleaned up but it wasn't", tmp2.exists());
    } finally {
      FileUtils.deleteQuietly(accumuloDir);
    }
  }

  @Test
  public void testCleanupIndexOpWithoutCommonParentVolume() throws IOException {
    File accumuloDir = Files.createTempDir();
    
    try {
      // Make some directories to simulate multiple volumes
      File v1 = new File(accumuloDir, "v1"), v2 = new File(accumuloDir, "v2");
      v1.mkdirs();
      v2.mkdirs();

      // And a "unique" tmp directory for each volume
      File tmp1 = new File(v1, "tmp"), tmp2 = new File(v2, "tmp");
      tmp1.mkdirs();
      tmp2.mkdirs();
      Path tmpPath1 = new Path(tmp1.toURI()), tmpPath2 = new Path(tmp2.toURI());
      
      HashMap<Property,String> testProps = new HashMap<Property,String>();
      testProps.put(Property.INSTANCE_VOLUMES, v1.toURI().toString() + "," + v2.toURI().toString());
      
      AccumuloConfiguration testConf = new FileUtilTestConfiguration(testProps);
      VolumeManager fs = VolumeManagerImpl.getLocal(accumuloDir.getAbsolutePath());
      
      FileUtil.cleanupIndexOp(testConf, tmpPath1, fs, new ArrayList<FileSKVIterator>());
      
      Assert.assertFalse("Expected " + tmp1 + " to be cleaned up but it wasn't", tmp1.exists());
      
      FileUtil.cleanupIndexOp(testConf, tmpPath2, fs, new ArrayList<FileSKVIterator>());
      
      Assert.assertFalse("Expected " + tmp2 + " to be cleaned up but it wasn't", tmp2.exists());
    } finally {
      FileUtils.deleteQuietly(accumuloDir);
    }
  }

  private static class FileUtilTestConfiguration extends AccumuloConfiguration {
    private DefaultConfiguration defaultConf = new DefaultConfiguration();
    private Map<Property,String> properties;
    
    public FileUtilTestConfiguration(Map<Property,String> properties) {
      this.properties = properties;
    }
    
    @Override
    public String get(Property property) {
      String value = properties.get(property);
      if (null != value) {
        return value;
      }
      return defaultConf.get(property);
    }

    @Override
    public void getProperties(Map<String,String> props, PropertyFilter filter) {
      throw new UnsupportedOperationException();
    }
    
  }
}
