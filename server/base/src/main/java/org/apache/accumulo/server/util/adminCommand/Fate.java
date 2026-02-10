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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.AdminUtil;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.fate.FateStore;
import org.apache.accumulo.core.fate.ReadOnlyFateStore;
import org.apache.accumulo.core.fate.user.UserFateStore;
import org.apache.accumulo.core.fate.zookeeper.MetaFateStore;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.accumulo.core.manager.thrift.FateService;
import org.apache.accumulo.core.manager.thrift.TFateId;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.Halt;
import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.accumulo.server.util.ServerKeywordExecutable;
import org.apache.accumulo.server.util.adminCommand.Fate.FateOpts;
import org.apache.accumulo.server.util.fateCommand.FateSummaryReport;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.accumulo.start.spi.UsageGroup;
import org.apache.accumulo.start.spi.UsageGroups;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;

@AutoService(KeywordExecutable.class)
public class Fate extends ServerKeywordExecutable<FateOpts> {

  private static final Logger LOG = LoggerFactory.getLogger(Fate.class);

  // This only exists because it is called from ITs
  public static void main(String[] args) throws Exception {
    new Fate().execute(args);
  }

  /**
   * Wrapper around the fate stores
   */
  private static class FateStores implements AutoCloseable {
    private final Map<FateInstanceType,FateStore<Fate>> storesMap;

    private FateStores(FateInstanceType type1, FateStore<Fate> store1, FateInstanceType type2,
        FateStore<Fate> store2) {
      storesMap = Map.of(type1, store1, type2, store2);
    }

    private Map<FateInstanceType,FateStore<Fate>> getStoresMap() {
      return storesMap;
    }

    @Override
    public void close() {
      for (var fs : storesMap.values()) {
        fs.close();
      }
    }
  }

  static class FateOpts extends ServerUtilOpts {
    @Parameter(description = "[<FateId>...]")
    List<String> fateIdList = new ArrayList<>();

    @Parameter(names = {"-c", "--cancel"},
        description = "<FateId>... Cancel new or submitted FaTE transactions")
    boolean cancel;

    @Parameter(names = {"-f", "--fail"},
        description = "<FateId>... Transition FaTE transaction status to FAILED_IN_PROGRESS")
    boolean fail;

    @Parameter(names = {"-d", "--delete"},
        description = "<FateId>... Delete FaTE transaction and its associated table locks")
    boolean delete;

    @Parameter(names = {"-p", "--print", "-print", "-l", "--list", "-list"},
        description = "[<FateId>...] Print information about FaTE transactions. Print only the FateId's specified or print all transactions if empty. Use -s to only print those with certain states. Use -t to only print those with certain FateInstanceTypes.")
    boolean print;

    @Parameter(names = "--summary",
        description = "[<FateId>...] Print a summary of FaTE transactions. Print only the FateId's specified or print all transactions if empty. Use -s to only print those with certain states. Use -t to only print those with certain FateInstanceTypes. Use -j to print the transactions in json.")
    boolean summarize;

    @Parameter(names = {"-j", "--json"},
        description = "Print transactions in json. Only useful for --summary command.")
    boolean printJson;

    @Parameter(names = {"-s", "--state"},
        description = "<state>... Print transactions in the state(s) {NEW, IN_PROGRESS, FAILED_IN_PROGRESS, FAILED, SUCCESSFUL}")
    List<String> states = new ArrayList<>();

    @Parameter(names = {"-t", "--type"},
        description = "<type>... Print transactions of fate instance type(s) {USER, META}")
    List<String> instanceTypes = new ArrayList<>();
  }

  private final CountDownLatch lockAcquiredLatch = new CountDownLatch(1);

  private class AdminLockWatcher implements ServiceLock.AccumuloLockWatcher {
    @Override
    public void lostLock(ServiceLock.LockLossReason reason) {
      String msg = "Admin lost lock: " + reason.toString();
      if (reason == ServiceLock.LockLossReason.LOCK_DELETED) {
        Halt.halt(0, msg);
      } else {
        Halt.halt(1, msg);
      }
    }

    @Override
    public void unableToMonitorLockNode(Exception e) {
      String msg = "Admin unable to monitor lock: " + e.getMessage();
      LOG.warn(msg);
      Halt.halt(1, msg);
    }

    @Override
    public void acquiredLock() {
      lockAcquiredLatch.countDown();
      LOG.debug("Acquired ZooKeeper lock for Admin");
    }

    @Override
    public void failedToAcquireLock(Exception e) {
      LOG.warn("Failed to acquire ZooKeeper lock for Admin, msg: " + e.getMessage());
    }
  }

  public Fate() {
    super(new FateOpts());
  }

  @Override
  public String keyword() {
    return "fate";
  }

  @Override
  public UsageGroup usageGroup() {
    return UsageGroups.ADMIN;
  }

  @Override
  public String description() {
    return "Operations performed on the Manager FaTE system.";
  }

  @Override
  public void execute(JCommander cl, FateOpts options) throws Exception {

    ServerContext context = options.getServerContext();
    validateFateUserInput(options);

    AdminUtil<Fate> admin = new AdminUtil<>();
    var zTableLocksPath = context.getServerPaths().createTableLocksPath();
    var zk = context.getZooSession();
    ServiceLock adminLock = null;
    Map<FateInstanceType,ReadOnlyFateStore<Fate>> readOnlyFateStores = null;

    try {
      if (options.cancel) {
        cancelSubmittedFateTxs(context, options.fateIdList);
      } else if (options.fail) {
        adminLock = createAdminLock(context);
        try (var fateStores = createFateStores(context, zk, adminLock)) {
          for (String fateIdStr : options.fateIdList) {
            if (!admin.prepFail(fateStores.getStoresMap(), fateIdStr)) {
              throw new AccumuloException("Could not fail transaction: " + fateIdStr);
            }
          }
        }
      } else if (options.delete) {
        adminLock = createAdminLock(context);
        try (var fateStores = createFateStores(context, zk, adminLock)) {
          for (String fateIdStr : options.fateIdList) {
            if (!admin.prepDelete(fateStores.getStoresMap(), fateIdStr)) {
              throw new AccumuloException("Could not delete transaction: " + fateIdStr);
            }
            admin.deleteLocks(zk, zTableLocksPath, fateIdStr);
          }
        }
      }

      if (options.print) {
        final Set<FateId> fateIdFilter = new TreeSet<>();
        options.fateIdList.forEach(fateIdStr -> fateIdFilter.add(FateId.from(fateIdStr)));
        EnumSet<ReadOnlyFateStore.TStatus> statusFilter = getCmdLineStatusFilters(options.states);
        EnumSet<FateInstanceType> typesFilter =
            getCmdLineInstanceTypeFilters(options.instanceTypes);
        readOnlyFateStores = createReadOnlyFateStores(context, zk);
        admin.print(readOnlyFateStores, zk, zTableLocksPath, new Formatter(System.out),
            fateIdFilter, statusFilter, typesFilter);
        // print line break at the end
        System.out.println();
      }

      if (options.summarize) {
        if (readOnlyFateStores == null) {
          readOnlyFateStores = createReadOnlyFateStores(context, zk);
        }
        summarizeFateTx(context, options, admin, readOnlyFateStores, zTableLocksPath);
      }
    } finally {
      if (adminLock != null) {
        adminLock.unlock();
      }
    }
  }

  private FateStores createFateStores(ServerContext context, ZooSession zk, ServiceLock adminLock)
      throws InterruptedException, KeeperException {
    var lockId = adminLock.getLockID();
    MetaFateStore<Fate> mfs = new MetaFateStore<>(zk, lockId, null);
    UserFateStore<Fate> ufs =
        new UserFateStore<>(context, SystemTables.FATE.tableName(), lockId, null);
    return new FateStores(FateInstanceType.META, mfs, FateInstanceType.USER, ufs);
  }

  private Map<FateInstanceType,ReadOnlyFateStore<Fate>> createReadOnlyFateStores(
      ServerContext context, ZooSession zk) throws InterruptedException, KeeperException {
    ReadOnlyFateStore<Fate> readOnlyMFS = new MetaFateStore<>(zk, null, null);
    ReadOnlyFateStore<Fate> readOnlyUFS =
        new UserFateStore<>(context, SystemTables.FATE.tableName(), null, null);
    return Map.of(FateInstanceType.META, readOnlyMFS, FateInstanceType.USER, readOnlyUFS);
  }

  private ServiceLock createAdminLock(ServerContext context) throws InterruptedException {
    var zk = context.getZooSession();
    UUID uuid = UUID.randomUUID();
    ServiceLockPath slp = context.getServerPaths().createAdminLockPath();
    ServiceLock adminLock = new ServiceLock(zk, slp, uuid);
    AdminLockWatcher lw = new AdminLockWatcher();
    ServiceLockData.ServiceDescriptors descriptors = new ServiceLockData.ServiceDescriptors();
    descriptors.addService(new ServiceLockData.ServiceDescriptor(uuid,
        ServiceLockData.ThriftService.NONE, "fake_admin_util_host", ResourceGroupId.DEFAULT));
    ServiceLockData sld = new ServiceLockData(descriptors);
    String lockPath = slp.toString();
    String parentLockPath = lockPath.substring(0, lockPath.lastIndexOf("/"));

    try {
      var zrw = zk.asReaderWriter();
      zrw.putPersistentData(parentLockPath, new byte[0], ZooUtil.NodeExistsPolicy.SKIP);
      zrw.putPersistentData(lockPath, new byte[0], ZooUtil.NodeExistsPolicy.SKIP);
    } catch (KeeperException | InterruptedException e) {
      throw new IllegalStateException("Error creating path in ZooKeeper", e);
    }

    adminLock.lock(lw, sld);
    lockAcquiredLatch.await();

    return adminLock;
  }

  private void validateFateUserInput(FateOpts cmd) {
    if (cmd.cancel && cmd.fail || cmd.cancel && cmd.delete || cmd.fail && cmd.delete) {
      throw new IllegalArgumentException(
          "Can only perform one of the following at a time: cancel, fail or delete.");
    }
    if ((cmd.cancel || cmd.fail || cmd.delete) && cmd.fateIdList.isEmpty()) {
      throw new IllegalArgumentException(
          "At least one txId required when using cancel, fail or delete");
    }
  }

  private void cancelSubmittedFateTxs(ServerContext context, List<String> fateIdList)
      throws AccumuloException {
    for (String fateIdStr : fateIdList) {
      FateId fateId = FateId.from(fateIdStr);
      TFateId thriftFateId = fateId.toThrift();
      boolean cancelled = cancelFateOperation(context, thriftFateId);
      if (cancelled) {
        System.out.println("FaTE transaction " + fateId + " was cancelled or already completed.");
      } else {
        System.out
            .println("FaTE transaction " + fateId + " was not cancelled, status may have changed.");
      }
    }
  }

  private boolean cancelFateOperation(ClientContext context, TFateId thriftFateId)
      throws AccumuloException {
    FateService.Client client = null;
    try {
      client = ThriftClientTypes.FATE.getConnectionWithRetry(context);
      return client.cancelFateOperation(TraceUtil.traceInfo(), context.rpcCreds(), thriftFateId);
    } catch (Exception e) {
      throw new AccumuloException(e);
    } finally {
      if (client != null) {
        ThriftUtil.close(client, context);
      }
    }
  }

  private void summarizeFateTx(ServerContext context, FateOpts cmd, AdminUtil<Fate> admin,
      Map<FateInstanceType,ReadOnlyFateStore<Fate>> fateStores, ServiceLockPath tableLocksPath)
      throws InterruptedException, AccumuloException, AccumuloSecurityException, KeeperException,
      NamespaceNotFoundException {

    var zk = context.getZooSession();
    var transactions = admin.getStatus(fateStores, zk, tableLocksPath, null, null, null);

    // build id map - relies on unique ids for tables and namespaces
    // used to look up the names of either table or namespace by id.
    Map<TableId,String> tidToNameMap = context.createTableIdToQualifiedNameMap();
    Map<String,String> idsToNameMap = new HashMap<>(tidToNameMap.size() * 2);
    tidToNameMap.forEach((tid, name) -> idsToNameMap.put(tid.canonical(), "t:" + name));
    context.namespaceOperations().namespaceIdMap().forEach((name, nsid) -> {
      String prev = idsToNameMap.put(nsid, "ns:" + name);
      if (prev != null) {
        LOG.warn("duplicate id found for table / namespace id. table name: {}, namespace name: {}",
            prev, name);
      }
    });

    Set<FateId> fateIdFilter =
        cmd.fateIdList.stream().map(FateId::from).collect(Collectors.toSet());
    EnumSet<ReadOnlyFateStore.TStatus> statusFilter = getCmdLineStatusFilters(cmd.states);
    EnumSet<FateInstanceType> typesFilter = getCmdLineInstanceTypeFilters(cmd.instanceTypes);

    FateSummaryReport report =
        new FateSummaryReport(idsToNameMap, fateIdFilter, statusFilter, typesFilter);

    // gather statistics
    transactions.getTransactions().forEach(report::gatherTxnStatus);
    if (cmd.printJson) {
      printLines(Collections.singletonList(report.toJson()));
    } else {
      printLines(report.formatLines());
    }
  }

  private void printLines(List<String> lines) {
    for (String nextLine : lines) {
      if (nextLine == null) {
        continue;
      }
      System.out.println(nextLine);
    }
  }

  /**
   * If provided on the command line, get the TStatus values provided.
   *
   * @return a set of status filters, or null if none provided
   */
  private EnumSet<ReadOnlyFateStore.TStatus> getCmdLineStatusFilters(List<String> states) {
    EnumSet<ReadOnlyFateStore.TStatus> statusFilter = null;
    if (!states.isEmpty()) {
      statusFilter = EnumSet.noneOf(ReadOnlyFateStore.TStatus.class);
      for (String element : states) {
        statusFilter.add(ReadOnlyFateStore.TStatus.valueOf(element));
      }
    }
    return statusFilter;
  }

  /**
   * If provided on the command line, get the FateInstanceType values provided.
   *
   * @return a set of fate instance types filters, or null if none provided
   */
  private EnumSet<FateInstanceType> getCmdLineInstanceTypeFilters(List<String> instanceTypes) {
    EnumSet<FateInstanceType> typesFilter = null;
    if (!instanceTypes.isEmpty()) {
      typesFilter = EnumSet.noneOf(FateInstanceType.class);
      for (String instanceType : instanceTypes) {
        typesFilter.add(FateInstanceType.valueOf(instanceType));
      }
    }
    return typesFilter;
  }

  /**
   * Finds tablets that point to fate operations that do not exists or are complete.
   *
   * @param tablets the tablets to inspect
   * @param tabletLookup a function that can lookup a tablets latest metadata
   * @param activePredicate a predicate that can determine if a fate id is currently active
   * @param danglingConsumer a consumer that tablets with inactive fate ids will be sent to
   */
  static void findDanglingFateOperations(Iterable<TabletMetadata> tablets,
      Function<Collection<KeyExtent>,Map<KeyExtent,TabletMetadata>> tabletLookup,
      Predicate<FateId> activePredicate, BiConsumer<KeyExtent,Set<FateId>> danglingConsumer,
      int bufferSize) {

    ArrayList<FateId> fateIds = new ArrayList<>();
    Map<KeyExtent,Set<FateId>> candidates = new HashMap<>();
    for (TabletMetadata tablet : tablets) {
      fateIds.clear();
      getAllFateIds(tablet, fateIds::add);
      fateIds.removeIf(activePredicate);
      if (!fateIds.isEmpty()) {
        candidates.put(tablet.getExtent(), new HashSet<>(fateIds));
        if (candidates.size() > bufferSize) {
          processCandidates(candidates, tabletLookup, danglingConsumer);
          candidates.clear();
        }
      }
    }

    processCandidates(candidates, tabletLookup, danglingConsumer);
  }

  private static void processCandidates(Map<KeyExtent,Set<FateId>> candidates,
      Function<Collection<KeyExtent>,Map<KeyExtent,TabletMetadata>> tabletLookup,
      BiConsumer<KeyExtent,Set<FateId>> danglingConsumer) {
    // Perform a 2nd check of the tablet to avoid race conditions like the following.
    // 1. THREAD 1 : TabletMetadata is read and points to active fate operation
    // 2. THREAD 2 : The fate operation is deleted from the tablet
    // 3. THREAD 2 : The fate operation completes
    // 4. THREAD 1 : Checks if the fate operation read in step 1 is active and finds it is not

    Map<KeyExtent,TabletMetadata> currentTablets = tabletLookup.apply(candidates.keySet());
    HashSet<FateId> currentFateIds = new HashSet<>();
    candidates.forEach((extent, fateIds) -> {
      var currentTablet = currentTablets.get(extent);
      if (currentTablet != null) {
        currentFateIds.clear();
        getAllFateIds(currentTablet, currentFateIds::add);
        // Only keep fate ids that are still present in the tablet. Any new fate ids in
        // currentFateIds that were not seen on the first pass are not considered here. To check
        // those new ones, the entire two-step process would need to be rerun.
        fateIds.retainAll(currentFateIds);

        if (!fateIds.isEmpty()) {
          // the fateIds in this set were found to be inactive and still exist in the tablet
          // metadata after being found inactive
          danglingConsumer.accept(extent, fateIds);
        }
      } // else the tablet no longer exist so nothing to report
    });
  }

  /**
   * Extracts all fate ids that a tablet points to from any field.
   */
  private static void getAllFateIds(TabletMetadata tabletMetadata,
      Consumer<FateId> fateIdConsumer) {
    tabletMetadata.getLoaded().values().forEach(fateIdConsumer);
    if (tabletMetadata.getSelectedFiles() != null) {
      fateIdConsumer.accept(tabletMetadata.getSelectedFiles().getFateId());
    }
    if (tabletMetadata.getOperationId() != null) {
      fateIdConsumer.accept(tabletMetadata.getOperationId().getFateId());
    }
  }

}
