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
package org.apache.accumulo.server.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.Halt;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.core.util.threads.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class FileSystemMonitor {
  private static final String PROC_MOUNTS = "/proc/mounts";
  private static final Logger log = LoggerFactory.getLogger(FileSystemMonitor.class);
  private static final String DEVICE_PREFIX = "/dev/";
  private static final Set<String> ACCEPTED_FILESYSTEMS = Sets.newHashSet("ext3", "ext4", "xfs");

  static class Mount {
    String device;
    String mountPoint;
    String filesystemType;
    Set<String> options;

    Mount(String line) {
      String[] tokens = line.split("\\s+");

      device = tokens[0].trim();
      mountPoint = tokens[1].trim();
      filesystemType = tokens[2].trim().toLowerCase();

      options = new HashSet<>(Arrays.asList(tokens[3].split(",")));
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder(32);
      sb.append(device).append(" on ").append(mountPoint).append(" as ").append(filesystemType)
          .append(" with options ").append(options);
      return sb.toString();
    }
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "procFile path not from user input")
  static List<Mount> parse(String procFile) throws IOException {

    FileReader fr = new FileReader(procFile, UTF_8);

    try (BufferedReader br = new BufferedReader(fr)) {
      return getMountsFromFile(br);
    }
  }

  static List<Mount> getMountsFromFile(BufferedReader br) throws IOException {
    List<Mount> mounts = new ArrayList<>();
    String line;
    while ((line = br.readLine()) != null) {
      Mount mount = new Mount(line);
      if (mount.device.startsWith(DEVICE_PREFIX)
          && ACCEPTED_FILESYSTEMS.contains(mount.filesystemType)) {
        log.trace("Retaining mount to check: '{}'", mount);
        mounts.add(mount);
      } else {
        log.trace("Ignoring mount to check: '{}'", mount);
      }
    }
    return mounts;
  }

  private Map<String,Boolean> readWriteFilesystems = new HashMap<>();

  public FileSystemMonitor(final String procFile, long period, AccumuloConfiguration conf)
      throws IOException {
    List<Mount> mounts = parse(procFile);

    // Populate readWriteFilesystems with the initial state of the mounts
    for (Mount mount : mounts) {
      if (mount.options.contains("rw"))
        readWriteFilesystems.put(mount.mountPoint, true);
      else if (mount.options.contains("ro"))
        readWriteFilesystems.put(mount.mountPoint, false);
      else
        throw new IOException("Filesystem " + mount + " does not have ro or rw option");
    }

    // Create a task to check each mount periodically to see if its state has changed.
    for (Mount mount : mounts) {
      ThreadPools.createGeneralScheduledExecutorService(conf).scheduleWithFixedDelay(
          Threads.createNamedRunnable(mount.mountPoint + "filesystem monitor", () -> {
            try {
              checkMount(mount);
            } catch (final Exception e) {
              Halt.halt(-42, new Runnable() {
                @Override
                public void run() {
                  log.error("Exception while checking mount points, halting process", e);
                }
              });
            }
          }), period, period, TimeUnit.MILLISECONDS);

    }

  }

  protected void checkMount(Mount mount) throws Exception {
    if (!readWriteFilesystems.containsKey(mount.mountPoint))
      if (mount.options.contains("rw"))
        readWriteFilesystems.put(mount.mountPoint, true);
      else if (mount.options.contains("ro"))
        readWriteFilesystems.put(mount.mountPoint, false);
      else
        throw new Exception("Filesystem " + mount + " does not have ro or rw option");
    else if (mount.options.contains("ro") && readWriteFilesystems.get(mount.mountPoint))
      throw new Exception("Filesystem " + mount.mountPoint + " switched to read only");
  }

  public static void start(AccumuloConfiguration conf, Property prop) {
    if (conf.getBoolean(prop)) {
      if (new File(PROC_MOUNTS).exists()) {
        try {
          new FileSystemMonitor(PROC_MOUNTS, 60000, conf);
          log.info("Filesystem monitor started");
        } catch (IOException e) {
          log.error("Failed to initialize file system monitor", e);
        }
      } else {
        log.info("Not monitoring filesystems, " + PROC_MOUNTS + " does not exists");
      }
    }
  }
}
