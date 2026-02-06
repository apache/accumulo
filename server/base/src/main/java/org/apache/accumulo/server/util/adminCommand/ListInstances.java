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
package org.apache.accumulo.server.util.adminCommand;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Formattable;
import java.util.Formatter;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.fate.zookeeper.ZooReader;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.lock.ServiceLockData.ThriftService;
import org.apache.accumulo.core.lock.ServiceLockPaths;
import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.accumulo.server.util.ServerKeywordExecutable;
import org.apache.accumulo.server.util.adminCommand.ListInstances.Opts;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;

@AutoService(KeywordExecutable.class)
public class ListInstances extends ServerKeywordExecutable<Opts> {

  private static final Logger log = LoggerFactory.getLogger(ListInstances.class);

  private static final int NAME_WIDTH = 20;
  private static final int UUID_WIDTH = 37;
  private static final int MANAGER_WIDTH = 30;

  private static final int ZOOKEEPER_TIMER_MILLIS = 30_000;

  static class Opts extends ServerUtilOpts {
    @Parameter(names = "--print-errors", description = "display errors while listing instances")
    boolean printErrors = false;
    @Parameter(names = "--print-all",
        description = "print information for all instances, not just those with names")
    boolean printAll = false;
    @Parameter(names = {"-z", "--zookeepers"}, description = "the zookeepers to contact")
    String keepers = null;
  }

  static int errors = 0;

  public ListInstances() {
    super(new Opts());
  }

  @Override
  public String keyword() {
    return "list-instances";
  }

  @Override
  public UsageGroup usageGroup() {
    return UsageGroup.ADMIN;
  }

  @Override
  public String description() {
    return "List Accumulo instances in zookeeper";
  }

  @Override
  public void execute(JCommander cl, Opts options) throws Exception {
    if (options.keepers == null) {
      options.keepers =
          options.getServerContext().getConfiguration().get(Property.INSTANCE_ZK_HOST);
    }

    listInstances(options.keepers, options.printAll, options.printErrors);
  }

  synchronized void listInstances(String keepers, boolean printAll, boolean printErrors)
      throws InterruptedException {
    errors = 0;

    System.out.println("INFO : Using ZooKeepers " + keepers);
    try (var zk = new ZooSession(ListInstances.class.getSimpleName(), keepers,
        ZOOKEEPER_TIMER_MILLIS, null)) {
      ZooReader rdr = zk.asReader();

      TreeMap<String,InstanceId> instanceNames = getInstanceNames(rdr, printErrors);

      System.out.println();
      printHeader();

      for (Entry<String,InstanceId> entry : instanceNames.entrySet()) {
        printInstanceInfo(zk, entry.getKey(), entry.getValue(), printErrors);
      }

      TreeSet<InstanceId> instancedIds = getInstanceIDs(rdr, printErrors);
      instancedIds.removeAll(instanceNames.values());

      if (printAll) {
        for (InstanceId uuid : instancedIds) {
          printInstanceInfo(zk, null, uuid, printErrors);
        }
      } else if (!instancedIds.isEmpty()) {
        System.out.println();
        System.out.println("INFO : " + instancedIds.size()
            + " unnamed instances were not printed, run with --print-all to see all instances");
      } else {
        System.out.println();
      }

      if (!printErrors && errors > 0) {
        System.err.println(
            "WARN : There were " + errors + " errors, run with --print-errors to see more info");
      }
    }
  }

  private class CharFiller implements Formattable {

    char c;

    CharFiller(char c) {
      this.c = c;
    }

    @Override
    public void formatTo(Formatter formatter, int flags, int width, int precision) {
      formatter.format(String.valueOf(c).repeat(Math.max(0, width)));
    }

  }

  private void printHeader() {
    System.out.printf(" %-" + NAME_WIDTH + "s| %-" + UUID_WIDTH + "s| %-" + MANAGER_WIDTH + "s%n",
        "Instance Name", "Instance ID", "Manager");
    System.out.printf(
        "%" + (NAME_WIDTH + 1) + "s+%" + (UUID_WIDTH + 1) + "s+%" + (MANAGER_WIDTH + 1) + "s%n",
        new CharFiller('-'), new CharFiller('-'), new CharFiller('-'));

  }

  private void printInstanceInfo(ZooSession zs, String instanceName, InstanceId iid,
      boolean printErrors) {
    String manager = getManager(zs, iid, printErrors);
    if (instanceName == null) {
      instanceName = "";
    }

    if (manager == null) {
      manager = "";
    }

    System.out.printf("%" + NAME_WIDTH + "s |%" + UUID_WIDTH + "s |%" + MANAGER_WIDTH + "s%n",
        "\"" + instanceName + "\"", iid, manager);
  }

  private String getManager(ZooSession zs, InstanceId iid, boolean printErrors) {

    if (iid == null) {
      return null;
    }

    try {
      var zLockManagerPath =
          ServiceLockPaths.parse(Optional.of(Constants.ZMANAGER_LOCK), Constants.ZMANAGER_LOCK);
      Optional<ServiceLockData> sld = ServiceLock.getLockData(zs, zLockManagerPath);
      if (sld.isEmpty()) {
        return null;
      }
      return sld.orElseThrow().getAddressString(ThriftService.MANAGER);
    } catch (Exception e) {
      handleException(e, printErrors);
      return null;
    }
  }

  private TreeMap<String,InstanceId> getInstanceNames(ZooReader zk, boolean printErrors) {

    String instancesPath = Constants.ZROOT + Constants.ZINSTANCES;

    TreeMap<String,InstanceId> tm = new TreeMap<>();

    List<String> names;

    try {
      names = zk.getChildren(instancesPath);
    } catch (Exception e) {
      handleException(e, printErrors);
      return tm;
    }

    for (String name : names) {
      String instanceNamePath = Constants.ZROOT + Constants.ZINSTANCES + "/" + name;
      try {
        InstanceId iid = InstanceId.of(new String(zk.getData(instanceNamePath), UTF_8));
        tm.put(name, iid);
      } catch (Exception e) {
        handleException(e, printErrors);
        tm.put(name, null);
      }
    }

    return tm;
  }

  private TreeSet<InstanceId> getInstanceIDs(ZooReader zk, boolean printErrors) {
    TreeSet<InstanceId> ts = new TreeSet<>();

    try {
      List<String> children = zk.getChildren(Constants.ZROOT);

      for (String iid : children) {
        if (iid.equals("instances")) {
          continue;
        }
        try {
          ts.add(InstanceId.of(iid));
        } catch (Exception e) {
          log.error("Exception: ", e);
        }
      }
    } catch (Exception e) {
      handleException(e, printErrors);
    }

    return ts;
  }

  private void handleException(Exception e, boolean printErrors) {
    if (printErrors) {
      log.error("{}", e.getMessage(), e);
    }

    errors++;
  }
}
