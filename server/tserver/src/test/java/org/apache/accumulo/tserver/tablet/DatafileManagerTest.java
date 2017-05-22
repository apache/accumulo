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
package org.apache.accumulo.tserver.tablet;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.fs.FileRef;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for org.apache.accumulo.tserver.tablet.DatafileManager
 */
public class DatafileManagerTest {
  private Tablet tablet;
  private KeyExtent extent;
  private TableConfiguration tableConf;

  private SortedMap<FileRef,DataFileValue> createFileMap(String... sa) {
    SortedMap<FileRef,DataFileValue> ret = new TreeMap<>();
    for (int i = 0; i < sa.length; i += 2) {
      ret.put(new FileRef("hdfs://nn1/accumulo/tables/5/t-0001/" + sa[i]), new DataFileValue(ConfigurationTypeHelper.getFixedMemoryAsBytes(sa[i + 1]), 1));
    }
    return ret;
  }

  @Before
  public void setupMockClasses() {
    tablet = EasyMock.createMock(Tablet.class);
    extent = EasyMock.createMock(KeyExtent.class);
    tableConf = EasyMock.createMock(TableConfiguration.class);

    EasyMock.expect(tablet.getExtent()).andReturn(extent);
    EasyMock.expect(tablet.getTableConfiguration()).andReturn(tableConf);
    EasyMock.expect(tableConf.getMaxFilesPerTablet()).andReturn(5);
  }

  /*
   * Test max file size (table.compaction.minor.merge.file.size.max) exceeded when calling reserveMergingMinorCompactionFile
   */
  @Test
  public void testReserveMergingMinorCompactionFile_MaxExceeded() throws IOException {
    String maxMergeFileSize = "1000B";
    EasyMock.expect(tablet.getTableConfiguration()).andReturn(tableConf);
    EasyMock.expect(tableConf.get(Property.TABLE_MINC_MAX_MERGE_FILE_SIZE)).andReturn(maxMergeFileSize);
    EasyMock.replay(tablet, tableConf);

    SortedMap<FileRef,DataFileValue> testFiles = createFileMap("largefile", "10M", "file2", "100M", "file3", "100M", "file4", "100M", "file5", "100M");

    DatafileManager dfm = new DatafileManager(tablet, testFiles);
    FileRef mergeFile = dfm.reserveMergingMinorCompactionFile();

    EasyMock.verify(tablet, tableConf);

    assertEquals(null, mergeFile);
  }

  /*
   * Test max files not reached (table.file.max) when calling reserveMergingMinorCompactionFile
   */
  @Test
  public void testReserveMergingMinorCompactionFile_MaxFilesNotReached() throws IOException {
    EasyMock.replay(tablet, tableConf);

    SortedMap<FileRef,DataFileValue> testFiles = createFileMap("smallfile", "100B", "file2", "100M", "file3", "100M", "file4", "100M");

    DatafileManager dfm = new DatafileManager(tablet, testFiles);
    FileRef mergeFile = dfm.reserveMergingMinorCompactionFile();

    EasyMock.verify(tablet, tableConf);

    assertEquals(null, mergeFile);
  }

  /*
   * Test the smallest file is chosen for merging minor compaction
   */
  @Test
  public void testReserveMergingMinorCompactionFile() throws IOException {
    String maxMergeFileSize = "1000B";
    EasyMock.expect(tablet.getTableConfiguration()).andReturn(tableConf);
    EasyMock.expect(tableConf.get(Property.TABLE_MINC_MAX_MERGE_FILE_SIZE)).andReturn(maxMergeFileSize);
    EasyMock.replay(tablet, tableConf);

    SortedMap<FileRef,DataFileValue> testFiles = createFileMap("smallfile", "100B", "file2", "100M", "file3", "100M", "file4", "100M", "file5", "100M");

    DatafileManager dfm = new DatafileManager(tablet, testFiles);
    FileRef mergeFile = dfm.reserveMergingMinorCompactionFile();

    EasyMock.verify(tablet, tableConf);

    assertEquals("smallfile", mergeFile.path().getName());
  }

  /*
   * Test disabled max file size for merging minor compaction
   */
  @Test
  public void testReserveMergingMinorCompactionFileDisabled() throws IOException {
    String maxMergeFileSize = "0";
    EasyMock.expect(tablet.getTableConfiguration()).andReturn(tableConf);
    EasyMock.expect(tableConf.get(Property.TABLE_MINC_MAX_MERGE_FILE_SIZE)).andReturn(maxMergeFileSize);
    EasyMock.replay(tablet, tableConf);

    SortedMap<FileRef,DataFileValue> testFiles = createFileMap("smallishfile", "10M", "file2", "100M", "file3", "100M", "file4", "100M", "file5", "100M");

    DatafileManager dfm = new DatafileManager(tablet, testFiles);
    FileRef mergeFile = dfm.reserveMergingMinorCompactionFile();

    EasyMock.verify(tablet, tableConf);

    assertEquals("smallishfile", mergeFile.path().getName());
  }

}
