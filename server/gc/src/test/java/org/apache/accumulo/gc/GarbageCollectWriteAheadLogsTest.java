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
package org.apache.accumulo.gc;

import com.google.common.net.HostAndPort;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;

import java.io.FileNotFoundException;
import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.gc.thrift.GCStatus;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import org.junit.Before;
import org.junit.Test;

import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.gc.thrift.GcCycleStats;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.zookeeper.KeeperException;

import java.io.File;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import static org.easymock.EasyMock.createMock;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static java.lang.Thread.sleep;

import java.io.FileOutputStream;

import org.apache.commons.io.FileUtils;

import java.util.concurrent.TimeUnit;

public class GarbageCollectWriteAheadLogsTest {
  private static final long BLOCK_SIZE = 64000000L;

  private static final Path DIR_1_PATH = new Path("/dir1");
  private static final Path DIR_2_PATH = new Path("/dir2");
  private static final Path DIR_3_PATH = new Path("/dir3");
  private static final String UUID1 = UUID.randomUUID().toString();
  private static final String UUID2 = UUID.randomUUID().toString();
  private static final String UUID3 = UUID.randomUUID().toString();

  private Instance instance;
  private VolumeManager volMgr;
  private GarbageCollectWriteAheadLogs gcwal;
  private long modTime;

  @Before
  public void setUp() throws Exception {
    instance = createMock(Instance.class);
    volMgr = createMock(VolumeManager.class);
    gcwal = new GarbageCollectWriteAheadLogs(instance, volMgr, false);
    modTime = System.currentTimeMillis();
  }

  @Test
  public void testGetters() {
    assertSame(instance, gcwal.getInstance());
    assertSame(volMgr, gcwal.getVolumeManager());
    assertFalse(gcwal.isUsingTrash());
  }

  @Test
  public void testPathsToStrings() {
    ArrayList<Path> paths = new ArrayList<Path>();
    paths.add(new Path(DIR_1_PATH, "file1"));
    paths.add(DIR_2_PATH);
    paths.add(new Path(DIR_3_PATH, "file3"));
    List<String> strings = GarbageCollectWriteAheadLogs.paths2strings(paths);
    int len = 3;
    assertEquals(len, strings.size());
    for (int i = 0; i < len; i++) {
      assertEquals(paths.get(i).toString(), strings.get(i));
    }
  }

  @Test
  public void testMapServersToFiles() {
    /*
     * Test fileToServerMap: /dir1/server1/uuid1 -> server1 (new-style) /dir1/uuid2 -> "" (old-style) /dir3/server3/uuid3 -> server3 (new-style)
     */
    Map<Path,String> fileToServerMap = new java.util.HashMap<Path,String>();
    Path path1 = new Path(new Path(DIR_1_PATH, "server1"), UUID1);
    fileToServerMap.put(path1, "server1"); // new-style
    Path path2 = new Path(DIR_1_PATH, UUID2);
    fileToServerMap.put(path2, ""); // old-style
    Path path3 = new Path(new Path(DIR_3_PATH, "server3"), UUID3);
    fileToServerMap.put(path3, "server3"); // old-style
    /*
     * Test nameToFileMap: uuid1 -> /dir1/server1/uuid1 uuid3 -> /dir3/server3/uuid3
     */
    Map<String,Path> nameToFileMap = new java.util.HashMap<String,Path>();
    nameToFileMap.put(UUID1, path1);
    nameToFileMap.put(UUID3, path3);

    /**
     * Expected map: server1 -> [ /dir1/server1/uuid1 ] server3 -> [ /dir3/server3/uuid3 ]
     */
    Map<String,ArrayList<Path>> result = GarbageCollectWriteAheadLogs.mapServersToFiles(fileToServerMap, nameToFileMap);
    assertEquals(2, result.size());
    ArrayList<Path> list1 = result.get("server1");
    assertEquals(1, list1.size());
    assertTrue(list1.contains(path1));
    ArrayList<Path> list3 = result.get("server3");
    assertEquals(1, list3.size());
    assertTrue(list3.contains(path3));
  }

  private FileStatus makeFileStatus(int size, Path path) {
    boolean isDir = (size == 0);
    return new FileStatus(size, isDir, 3, BLOCK_SIZE, modTime, path);
  }

  private void mockListStatus(Path dir, FileStatus... fileStatuses) throws Exception {
    expect(volMgr.listStatus(dir)).andReturn(fileStatuses);
  }

  @Test
  public void testScanServers_NewStyle() throws Exception {
    String[] walDirs = new String[] {"/dir1", "/dir2", "/dir3"};
    /*
     * Test directory layout: /dir1/ server1/ uuid1 file2 subdir2/ /dir2/ missing /dir3/ server3/ uuid3
     */
    Path serverDir1Path = new Path(DIR_1_PATH, "server1");
    FileStatus serverDir1 = makeFileStatus(0, serverDir1Path);
    Path subDir2Path = new Path(DIR_1_PATH, "subdir2");
    FileStatus serverDir2 = makeFileStatus(0, subDir2Path);
    mockListStatus(DIR_1_PATH, serverDir1, serverDir2);
    Path path1 = new Path(serverDir1Path, UUID1);
    FileStatus file1 = makeFileStatus(100, path1);
    FileStatus file2 = makeFileStatus(200, new Path(serverDir1Path, "file2"));
    mockListStatus(serverDir1Path, file1, file2);
    mockListStatus(subDir2Path);
    expect(volMgr.listStatus(DIR_2_PATH)).andThrow(new FileNotFoundException());
    Path serverDir3Path = new Path(DIR_3_PATH, "server3");
    FileStatus serverDir3 = makeFileStatus(0, serverDir3Path);
    mockListStatus(DIR_3_PATH, serverDir3);
    Path path3 = new Path(serverDir3Path, UUID3);
    FileStatus file3 = makeFileStatus(300, path3);
    mockListStatus(serverDir3Path, file3);
    replay(volMgr);

    Map<Path,String> fileToServerMap = new java.util.HashMap<Path,String>();
    Map<String,Path> nameToFileMap = new java.util.HashMap<String,Path>();
    int count = gcwal.scanServers(walDirs, fileToServerMap, nameToFileMap);
    assertEquals(3, count);
    /*
     * Expected fileToServerMap: /dir1/server1/uuid1 -> server1 /dir3/server3/uuid3 -> server3
     */
    assertEquals(2, fileToServerMap.size());
    assertEquals("server1", fileToServerMap.get(path1));
    assertEquals("server3", fileToServerMap.get(path3));
    /*
     * Expected nameToFileMap: uuid1 -> /dir1/server1/uuid1 uuid3 -> /dir3/server3/uuid3
     */
    assertEquals(2, nameToFileMap.size());
    assertEquals(path1, nameToFileMap.get(UUID1));
    assertEquals(path3, nameToFileMap.get(UUID3));
  }

  @Test
  public void testScanServers_OldStyle() throws Exception {
    /*
     * Test directory layout: /dir1/ uuid1 /dir3/ uuid3
     */
    String[] walDirs = new String[] {"/dir1", "/dir3"};
    Path serverFile1Path = new Path(DIR_1_PATH, UUID1);
    FileStatus serverFile1 = makeFileStatus(100, serverFile1Path);
    mockListStatus(DIR_1_PATH, serverFile1);
    Path serverFile3Path = new Path(DIR_3_PATH, UUID3);
    FileStatus serverFile3 = makeFileStatus(300, serverFile3Path);
    mockListStatus(DIR_3_PATH, serverFile3);
    replay(volMgr);

    Map<Path,String> fileToServerMap = new java.util.HashMap<Path,String>();
    Map<String,Path> nameToFileMap = new java.util.HashMap<String,Path>();
    int count = gcwal.scanServers(walDirs, fileToServerMap, nameToFileMap);
    /*
     * Expect only a single server, the non-server entry for upgrade WALs
     */
    assertEquals(1, count);
    /*
     * Expected fileToServerMap: /dir1/uuid1 -> "" /dir3/uuid3 -> ""
     */
    assertEquals(2, fileToServerMap.size());
    assertEquals("", fileToServerMap.get(serverFile1Path));
    assertEquals("", fileToServerMap.get(serverFile3Path));
    /*
     * Expected nameToFileMap: uuid1 -> /dir1/uuid1 uuid3 -> /dir3/uuid3
     */
    assertEquals(2, nameToFileMap.size());
    assertEquals(serverFile1Path, nameToFileMap.get(UUID1));
    assertEquals(serverFile3Path, nameToFileMap.get(UUID3));
  }

  @Test
  public void testGetSortedWALogs() throws Exception {
    String[] recoveryDirs = new String[] {"/dir1", "/dir2", "/dir3"};
    /*
     * Test directory layout: /dir1/ uuid1 file2 /dir2/ missing /dir3/ uuid3
     */
    expect(volMgr.exists(DIR_1_PATH)).andReturn(true);
    expect(volMgr.exists(DIR_2_PATH)).andReturn(false);
    expect(volMgr.exists(DIR_3_PATH)).andReturn(true);
    Path path1 = new Path(DIR_1_PATH, UUID1);
    FileStatus file1 = makeFileStatus(100, path1);
    FileStatus file2 = makeFileStatus(200, new Path(DIR_1_PATH, "file2"));
    mockListStatus(DIR_1_PATH, file1, file2);
    Path path3 = new Path(DIR_3_PATH, UUID3);
    FileStatus file3 = makeFileStatus(300, path3);
    mockListStatus(DIR_3_PATH, file3);
    replay(volMgr);

    Map<String,Path> sortedWalogs = gcwal.getSortedWALogs(recoveryDirs);
    /**
     * Expected map: uuid1 -> /dir1/uuid1 uuid3 -> /dir3/uuid3
     */
    assertEquals(2, sortedWalogs.size());
    assertEquals(path1, sortedWalogs.get(UUID1));
    assertEquals(path3, sortedWalogs.get(UUID3));
  }

  @Test
  public void testIsUUID() {
    assertTrue(GarbageCollectWriteAheadLogs.isUUID(UUID.randomUUID().toString()));
    assertFalse(GarbageCollectWriteAheadLogs.isUUID("foo"));
    assertFalse(GarbageCollectWriteAheadLogs.isUUID("0" + UUID.randomUUID().toString()));
    assertFalse(GarbageCollectWriteAheadLogs.isUUID(null));
  }

  @Test
  public void testTimeToDeleteTrue() throws InterruptedException {
    HostAndPort address = HostAndPort.fromString("tserver1:9998");
    long wait = AccumuloConfiguration.getTimeInMillis("1s");
    gcwal.clearFirstSeenDead();
    assertFalse("First call should be false and should store the first seen time", gcwal.timeToDelete(address, wait));
    sleep(wait * 2);
    assertTrue(gcwal.timeToDelete(address, wait));
  }

  @Test
  public void testTimeToDeleteFalse() {
    HostAndPort address = HostAndPort.fromString("tserver1:9998");
    long wait = AccumuloConfiguration.getTimeInMillis("1h");
    long t1, t2;
    boolean ttd;
    do {
      t1 = System.nanoTime();
      gcwal.clearFirstSeenDead();
      assertFalse("First call should be false and should store the first seen time", gcwal.timeToDelete(address, wait));
      ttd = gcwal.timeToDelete(address, wait);
      t2 = System.nanoTime();
    } while (TimeUnit.NANOSECONDS.toMillis(t2 - t1) > (wait / 2)); // as long as it took less than half of the configured wait

    assertFalse(ttd);
  }

  @Test
  public void testTimeToDeleteWithNullAddress() {
    assertFalse(gcwal.timeToDelete(null, 123l));
  }

  /**
   * Wrapper class with some helper methods
   * <p>
   * Just a wrapper around a LinkedHashMap that store method name and argument information. Also includes some convenience methods to make usage cleaner.
   */
  class MethodCalls {

    private LinkedHashMap<String,List<Object>> mapWrapper;

    public MethodCalls() {
      mapWrapper = new LinkedHashMap<String,List<Object>>();
    }

    public void put(String methodName, Object... args) {
      mapWrapper.put(methodName, Arrays.asList(args));
    }

    public int size() {
      return mapWrapper.size();
    }

    public boolean hasOneEntry() {
      return size() == 1;
    }

    public Map.Entry<String,List<Object>> getFirstEntry() {
      return mapWrapper.entrySet().iterator().next();
    }

    public String getFirstEntryMethod() {
      return getFirstEntry().getKey();
    }

    public List<Object> getFirstEntryArgs() {
      return getFirstEntry().getValue();
    }

    public Object getFirstEntryArg(int number) {
      return getFirstEntryArgs().get(number);
    }
  }

  /**
   * Partial mock of the GarbageCollectWriteAheadLogs for testing the removeFile method
   * <p>
   * There is a map named methodCalls that can be used to assert parameters on methods called inside the removeFile method
   */
  class GCWALPartialMock extends GarbageCollectWriteAheadLogs {

    private boolean holdsLockBool = false;

    public GCWALPartialMock(Instance i, VolumeManager vm, boolean useTrash, boolean holdLock) throws IOException {
      super(i, vm, useTrash);
      this.holdsLockBool = holdLock;
    }

    public MethodCalls methodCalls = new MethodCalls();

    @Override
    boolean holdsLock(HostAndPort addr) {
      return holdsLockBool;
    }

    @Override
    void removeWALfromDownTserver(HostAndPort address, AccumuloConfiguration conf, Entry<String,ArrayList<Path>> entry, final GCStatus status) {
      methodCalls.put("removeWALFromDownTserver", address, conf, entry, status);
    }

    @Override
    void askTserverToRemoveWAL(HostAndPort address, AccumuloConfiguration conf, Entry<String,ArrayList<Path>> entry, final GCStatus status) {
      methodCalls.put("askTserverToRemoveWAL", address, conf, entry, status);
    }

    @Override
    void removeOldStyleWAL(Entry<String,ArrayList<Path>> entry, final GCStatus status) {
      methodCalls.put("removeOldStyleWAL", entry, status);
    }

    @Override
    void removeSortedWAL(Path swalog) {
      methodCalls.put("removeSortedWAL", swalog);
    }
  }

  private GCWALPartialMock getGCWALForRemoveFileTest(GCStatus s, final boolean locked) throws IOException {
    return new GCWALPartialMock(new MockInstance("accumulo"), VolumeManagerImpl.get(), false, locked);
  }

  private Map<String,Path> getEmptyMap() {
    return new HashMap<String,Path>();
  }

  private Map<String,ArrayList<Path>> getServerToFileMap1(String key, Path singlePath) {
    Map<String,ArrayList<Path>> serverToFileMap = new HashMap<String,ArrayList<Path>>();
    serverToFileMap.put(key, new ArrayList<Path>(Arrays.asList(singlePath)));
    return serverToFileMap;
  }

  @Test
  public void testRemoveFilesWithOldStyle() throws IOException {
    GCStatus status = new GCStatus();
    GarbageCollectWriteAheadLogs realGCWAL = getGCWALForRemoveFileTest(status, true);
    Path p1 = new Path("hdfs://localhost:9000/accumulo/wal/tserver1+9997/" + UUID.randomUUID().toString());
    Map<String,ArrayList<Path>> serverToFileMap = getServerToFileMap1("", p1);

    realGCWAL.removeFiles(getEmptyMap(), serverToFileMap, getEmptyMap(), status);

    MethodCalls calls = ((GCWALPartialMock) realGCWAL).methodCalls;
    assertEquals("Only one method should have been called", 1, calls.size());
    assertEquals("Method should be removeOldStyleWAL", "removeOldStyleWAL", calls.getFirstEntryMethod());
    Entry<String,ArrayList<Path>> firstServerToFileMap = serverToFileMap.entrySet().iterator().next();
    assertEquals("First param should be empty", firstServerToFileMap, calls.getFirstEntryArg(0));
    assertEquals("Second param should be the status", status, calls.getFirstEntryArg(1));
  }

  @Test
  public void testRemoveFilesWithDeadTservers() throws IOException {
    GCStatus status = new GCStatus();
    GarbageCollectWriteAheadLogs realGCWAL = getGCWALForRemoveFileTest(status, false);
    String server = "tserver1+9997";
    Path p1 = new Path("hdfs://localhost:9000/accumulo/wal/" + server + "/" + UUID.randomUUID().toString());
    Map<String,ArrayList<Path>> serverToFileMap = getServerToFileMap1(server, p1);

    realGCWAL.removeFiles(getEmptyMap(), serverToFileMap, getEmptyMap(), status);

    MethodCalls calls = ((GCWALPartialMock) realGCWAL).methodCalls;
    assertEquals("Only one method should have been called", 1, calls.size());
    assertEquals("Method should be removeWALfromDownTserver", "removeWALFromDownTserver", calls.getFirstEntryMethod());
    assertEquals("First param should be address", HostAndPort.fromString(server.replaceAll("[+]", ":")), calls.getFirstEntryArg(0));
    assertTrue("Second param should be an AccumuloConfiguration", calls.getFirstEntryArg(1) instanceof AccumuloConfiguration);
    Entry<String,ArrayList<Path>> firstServerToFileMap = serverToFileMap.entrySet().iterator().next();
    assertEquals("Third param should be the entry", firstServerToFileMap, calls.getFirstEntryArg(2));
    assertEquals("Forth param should be the status", status, calls.getFirstEntryArg(3));
  }

  @Test
  public void testRemoveFilesWithLiveTservers() throws IOException {
    GCStatus status = new GCStatus();
    GarbageCollectWriteAheadLogs realGCWAL = getGCWALForRemoveFileTest(status, true);
    String server = "tserver1+9997";
    Path p1 = new Path("hdfs://localhost:9000/accumulo/wal/" + server + "/" + UUID.randomUUID().toString());
    Map<String,ArrayList<Path>> serverToFileMap = getServerToFileMap1(server, p1);

    realGCWAL.removeFiles(getEmptyMap(), serverToFileMap, getEmptyMap(), status);

    MethodCalls calls = ((GCWALPartialMock) realGCWAL).methodCalls;
    assertEquals("Only one method should have been called", 1, calls.size());
    assertEquals("Method should be askTserverToRemoveWAL", "askTserverToRemoveWAL", calls.getFirstEntryMethod());
    assertEquals("First param should be address", HostAndPort.fromString(server.replaceAll("[+]", ":")), calls.getFirstEntryArg(0));
    assertTrue("Second param should be an AccumuloConfiguration", calls.getFirstEntryArg(1) instanceof AccumuloConfiguration);
    Entry<String,ArrayList<Path>> firstServerToFileMap = serverToFileMap.entrySet().iterator().next();
    assertEquals("Third param should be the entry", firstServerToFileMap, calls.getFirstEntryArg(2));
    assertEquals("Forth param should be the status", status, calls.getFirstEntryArg(3));
  }

  @Test
  public void testRemoveFilesRemovesSortedWALs() throws IOException {
    GCStatus status = new GCStatus();
    GarbageCollectWriteAheadLogs realGCWAL = getGCWALForRemoveFileTest(status, true);
    Map<String,ArrayList<Path>> serverToFileMap = new HashMap<String,ArrayList<Path>>();
    Map<String,Path> sortedWALogs = new HashMap<String,Path>();
    Path p1 = new Path("hdfs://localhost:9000/accumulo/wal/tserver1+9997/" + UUID.randomUUID().toString());
    sortedWALogs.put("junk", p1); // TODO: see if this key is actually used here, maybe can be removed

    realGCWAL.removeFiles(getEmptyMap(), serverToFileMap, sortedWALogs, status);
    MethodCalls calls = ((GCWALPartialMock) realGCWAL).methodCalls;
    assertEquals("Only one method should have been called", 1, calls.size());
    assertEquals("Method should be removeSortedWAL", "removeSortedWAL", calls.getFirstEntryMethod());
    assertEquals("First param should be the Path", p1, calls.getFirstEntryArg(0));

  }

  static String GCWAL_DEAD_DIR = "gcwal-collect-deadtserver";
  static String GCWAL_DEAD_TSERVER = "tserver1";
  static String GCWAL_DEAD_TSERVER_PORT = "9995";
  static String GCWAL_DEAD_TSERVER_COLLECT_FILE = UUID.randomUUID().toString();

  class GCWALDeadTserverCollectMock extends GarbageCollectWriteAheadLogs {

    public GCWALDeadTserverCollectMock(Instance i, VolumeManager vm, boolean useTrash) throws IOException {
      super(i, vm, useTrash);
    }

    @Override
    boolean holdsLock(HostAndPort addr) {
      // tries use zookeeper
      return false;
    }

    @Override
    Map<String,Path> getSortedWALogs() {
      return new HashMap<String,Path>();
    }

    @Override
    int scanServers(Map<Path,String> fileToServerMap, Map<String,Path> nameToFileMap) throws Exception {
      String sep = File.separator;
      Path p = new Path(System.getProperty("user.dir") + sep + "target" + sep + GCWAL_DEAD_DIR + sep + GCWAL_DEAD_TSERVER + "+" + GCWAL_DEAD_TSERVER_PORT + sep
          + GCWAL_DEAD_TSERVER_COLLECT_FILE);
      fileToServerMap.put(p, GCWAL_DEAD_TSERVER + ":" + GCWAL_DEAD_TSERVER_PORT);
      nameToFileMap.put(GCWAL_DEAD_TSERVER_COLLECT_FILE, p);
      return 1;
    }

    @Override
    int removeMetadataEntries(Map<String,Path> nameToFileMap, Map<String,Path> sortedWALogs, GCStatus status) throws IOException, KeeperException,
        InterruptedException {
      return 0;
    }

    long getGCWALDeadServerWaitTime(AccumuloConfiguration conf) {
      // tries to use zookeeper
      return 1000l;
    }
  }

  @Test
  public void testCollectWithDeadTserver() throws IOException, InterruptedException {
    Instance i = new MockInstance();
    File walDir = new File(System.getProperty("user.dir") + File.separator + "target" + File.separator + GCWAL_DEAD_DIR);
    File walFileDir = new File(walDir + File.separator + GCWAL_DEAD_TSERVER + "+" + GCWAL_DEAD_TSERVER_PORT);
    File walFile = new File(walFileDir + File.separator + GCWAL_DEAD_TSERVER_COLLECT_FILE);
    if (!walFileDir.exists()) {
      walFileDir.mkdirs();
      new FileOutputStream(walFile).close();
    }

    try {
      VolumeManager vm = VolumeManagerImpl.getLocal(walDir.toString());
      GarbageCollectWriteAheadLogs gcwal2 = new GCWALDeadTserverCollectMock(i, vm, false);
      GCStatus status = new GCStatus(new GcCycleStats(), new GcCycleStats(), new GcCycleStats(), new GcCycleStats());

      gcwal2.collect(status);

      assertTrue("File should not be deleted", walFile.exists());
      assertEquals("Should have one candidate", 1, status.lastLog.getCandidates());
      assertEquals("Should not have deleted that file", 0, status.lastLog.getDeleted());

      sleep(2000);
      gcwal2.collect(status);

      assertFalse("File should be gone", walFile.exists());
      assertEquals("Should have one candidate", 1, status.lastLog.getCandidates());
      assertEquals("Should have deleted that file", 1, status.lastLog.getDeleted());

    } finally {
      if (walDir.exists()) {
        FileUtils.deleteDirectory(walDir);
      }
    }
  }
}
