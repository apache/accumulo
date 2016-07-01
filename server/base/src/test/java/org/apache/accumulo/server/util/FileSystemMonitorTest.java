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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.server.util.FileSystemMonitor.Mount;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSystemMonitorTest {
  private static final Logger log = LoggerFactory.getLogger(FileSystemMonitorTest.class);

  @Test
  public void testFilteredMountEntries() throws Exception {
    String[] mountEntries = new String[] {"rootfs / rootfs rw 0 0", "proc /proc proc rw,nosuid,nodev,noexec,relatime 0 0",
        "sysfs /sys sysfs rw,seclabel,nosuid,nodev,noexec,relatime 0 0",
        "devtmpfs /dev devtmpfs rw,seclabel,nosuid,size=8119336k,nr_inodes=2029834,mode=755 0 0",
        "securityfs /sys/kernel/security securityfs rw,nosuid,nodev,noexec,relatime 0 0", "tmpfs /dev/shm tmpfs rw,seclabel,nosuid,nodev 0 0",
        "devpts /dev/pts devpts rw,seclabel,nosuid,noexec,relatime,gid=5,mode=620,ptmxmode=000 0 0", "tmpfs /run tmpfs rw,seclabel,nosuid,nodev,mode=755 0 0",
        "tmpfs /sys/fs/cgroup tmpfs ro,seclabel,nosuid,nodev,noexec,mode=755 0 0",
        "cgroup /sys/fs/cgroup/systemd cgroup rw,nosuid,nodev,noexec,relatime,xattr,release_agent=/usr/lib/systemd/systemd-cgroups-agent,name=systemd 0 0",
        "pstore /sys/fs/pstore pstore rw,nosuid,nodev,noexec,relatime 0 0", "cgroup /sys/fs/cgroup/cpuset cgroup rw,nosuid,nodev,noexec,relatime,cpuset 0 0",
        "cgroup /sys/fs/cgroup/cpu,cpuacct cgroup rw,nosuid,nodev,noexec,relatime,cpuacct,cpu 0 0",
        "cgroup /sys/fs/cgroup/memory cgroup rw,nosuid,nodev,noexec,relatime,memory 0 0",
        "cgroup /sys/fs/cgroup/devices cgroup rw,nosuid,nodev,noexec,relatime,devices 0 0",
        "cgroup /sys/fs/cgroup/freezer cgroup rw,nosuid,nodev,noexec,relatime,freezer 0 0",
        "cgroup /sys/fs/cgroup/net_cls cgroup rw,nosuid,nodev,noexec,relatime,net_cls 0 0",
        "cgroup /sys/fs/cgroup/blkio cgroup rw,nosuid,nodev,noexec,relatime,blkio 0 0",
        "cgroup /sys/fs/cgroup/perf_event cgroup rw,nosuid,nodev,noexec,relatime,perf_event 0 0",
        "cgroup /sys/fs/cgroup/hugetlb cgroup rw,nosuid,nodev,noexec,relatime,hugetlb 0 0", "configfs /sys/kernel/config configfs rw,relatime 0 0",
        "/dev/vda1 / xfs rw,seclabel,relatime,attr2,inode64,noquota 0 0", "/dev/vda2 /ignoreme reiserfs rw 0 0",
        "rpc_pipefs /var/lib/nfs/rpc_pipefs rpc_pipefs rw,relatime 0 0", "selinuxfs /sys/fs/selinux selinuxfs rw,relatime 0 0",
        "systemd-1 /proc/sys/fs/binfmt_misc autofs rw,relatime,fd=32,pgrp=1,timeout=300,minproto=5,maxproto=5,direct 0 0",
        "debugfs /sys/kernel/debug debugfs rw,relatime 0 0", "mqueue /dev/mqueue mqueue rw,seclabel,relatime 0 0",
        "hugetlbfs /dev/hugepages hugetlbfs rw,seclabel,relatime 0 0", "sunrpc /proc/fs/nfsd nfsd rw,relatime 0 0",
        "/dev/vdb /grid/0 ext4 rw,seclabel,relatime,data=ordered 0 0"};

    StringBuilder sb = new StringBuilder(256);
    for (String mountEntry : mountEntries) {
      if (sb.length() > 0) {
        sb.append("\n");
      }
      sb.append(mountEntry);
    }

    ByteArrayInputStream is = new ByteArrayInputStream(sb.toString().getBytes(UTF_8));
    BufferedReader reader = new BufferedReader(new InputStreamReader(is));

    List<Mount> mounts = FileSystemMonitor.getMountsFromFile(reader);
    log.info("Filtered mount points: " + mounts);
    assertEquals(2, mounts.size());
    Set<String> expectedCheckedMountPoints = new HashSet<>();
    expectedCheckedMountPoints.add("/");
    expectedCheckedMountPoints.add("/grid/0");
    for (Mount mount : mounts) {
      assertTrue("Did not expect to find " + mount, expectedCheckedMountPoints.remove(mount.mountPoint));
    }
    assertEquals("Should not have any extra mount points: " + expectedCheckedMountPoints, 0, expectedCheckedMountPoints.size());
  }

}
