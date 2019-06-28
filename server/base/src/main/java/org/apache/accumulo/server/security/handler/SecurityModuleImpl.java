package org.apache.accumulo.server.security.handler;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SecurityModuleImpl implements SecurityModule {
  private static final Logger log = LoggerFactory.getLogger(SecurityModuleImpl.class);
  private final String ZKUserAuths = "/Authorizations";
  private final String ZKUserSysPerms = "/System";
  private final String ZKUserTablePerms = "/Tables";
  private final String ZKUserNamespacePerms = "/Namespaces";

  private ServerContext context;
  private AuthImpl auth;
  private Perm perm;
  private ZooCache zooCache;
  private String ZKUserPath;

  public SecurityModuleImpl(ServerContext context) {
    this.context = context;
    this.ZKUserPath = ZKSecurityTool.getInstancePath(context.getInstanceID()) + "/users";
    this.zooCache = new ZooCache(context.getZooReaderWriter(), null);

    this.auth = new AuthImpl(zooCache, context, ZKUserPath);
    this.perm = new PermImpl();
  }

  // @Override
  public void initialize(String rootUser, byte[] token) {
    ZooReaderWriter zoo = context.getZooReaderWriter();

    // authenticator.initializeSecurity(rootPrincipal, token);
    try {
      // remove old settings from zookeeper first, if any
      synchronized (zooCache) {
        zooCache.clear();
        if (zoo.exists(ZKUserPath)) {
          zoo.recursiveDelete(ZKUserPath, ZooUtil.NodeMissingPolicy.SKIP);
          log.info("Removed {}/ from zookeeper", ZKUserPath);
        }

        // prep parent node of users with root username
        zoo.putPersistentData(ZKUserPath, rootUser.getBytes(UTF_8), ZooUtil.NodeExistsPolicy.FAIL);

        auth.constructUser(rootUser, ZKSecurityTool.createPass(token));
      }
    } catch (KeeperException | AccumuloException | InterruptedException e) {
      log.error("{}", e.getMessage(), e);
      throw new RuntimeException(e);
    }

    // authorizor.initializeSecurity(credentials, rootPrincipal);
    // create the root user with no record-level authorizations
    try {
      // prep parent node of users with root username
      if (!zoo.exists(ZKUserPath))
        zoo.putPersistentData(ZKUserPath, rootUser.getBytes(UTF_8), ZooUtil.NodeExistsPolicy.FAIL);

      initUser(zoo, rootUser);
      zoo.putPersistentData(ZKUserPath + "/" + rootUser + ZKUserAuths,
          ZKSecurityTool.convertAuthorizations(Authorizations.EMPTY),
          ZooUtil.NodeExistsPolicy.FAIL);
    } catch (KeeperException | InterruptedException | AccumuloSecurityException e) {
      log.error("{}", e.getMessage(), e);
      throw new RuntimeException(e);
    }

    // permHandle.initializeSecurity(credentials, rootPrincipal);
    // create the root user with all system privileges, no table privileges, and no record-level
    // authorizations
    Set<SystemPermission> rootPerms = new TreeSet<>();
    for (SystemPermission p : SystemPermission.values())
      rootPerms.add(p);
    Map<TableId,Set<TablePermission>> tablePerms = new HashMap<>();
    // Allow the root user to flush the system tables
    tablePerms.put(RootTable.ID, Collections.singleton(TablePermission.ALTER_TABLE));
    tablePerms.put(MetadataTable.ID, Collections.singleton(TablePermission.ALTER_TABLE));
    // essentially the same but on the system namespace, the ALTER_TABLE permission is now redundant
    Map<NamespaceId,Set<NamespacePermission>> namespacePerms = new HashMap<>();
    namespacePerms.put(Namespace.ACCUMULO.id(),
        Collections.singleton(NamespacePermission.ALTER_NAMESPACE));
    namespacePerms.put(Namespace.ACCUMULO.id(),
        Collections.singleton(NamespacePermission.ALTER_TABLE));

    try {
      // prep parent node of users with root username
      if (!zoo.exists(ZKUserPath))
        zoo.putPersistentData(ZKUserPath, rootUser.getBytes(UTF_8), ZooUtil.NodeExistsPolicy.FAIL);

      initUser(zoo, rootUser);
      zoo.putPersistentData(ZKUserPath + "/" + rootUser + ZKUserSysPerms,
          ZKSecurityTool.convertSystemPermissions(rootPerms), ZooUtil.NodeExistsPolicy.FAIL);
      for (Map.Entry<TableId,Set<TablePermission>> entry : tablePerms.entrySet())
        createTablePerm(zoo, rootUser, entry.getKey(), entry.getValue());
      for (Map.Entry<NamespaceId,Set<NamespacePermission>> entry : namespacePerms.entrySet())
        createNamespacePerm(zoo, rootUser, entry.getKey(), entry.getValue());
    } catch (KeeperException | InterruptedException | AccumuloSecurityException e) {
      log.error("{}", e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Sets up a new table configuration for the provided user/table. No checking for existence is
   * done here, it should be done before calling.
   */
  private void createTablePerm(ZooReaderWriter zoo, String user, TableId table,
      Set<TablePermission> perms) throws KeeperException, InterruptedException {
    synchronized (zooCache) {
      zooCache.clear();
      zoo.putPersistentData(ZKUserPath + "/" + user + ZKUserTablePerms + "/" + table,
          ZKSecurityTool.convertTablePermissions(perms), ZooUtil.NodeExistsPolicy.FAIL);
    }
  }

  /**
   * Sets up a new namespace configuration for the provided user/table. No checking for existence is
   * done here, it should be done before calling.
   */
  private void createNamespacePerm(ZooReaderWriter zoo, String user, NamespaceId namespace,
      Set<NamespacePermission> perms) throws KeeperException, InterruptedException {
    synchronized (zooCache) {
      zooCache.clear();
      zoo.putPersistentData(ZKUserPath + "/" + user + ZKUserNamespacePerms + "/" + namespace,
          ZKSecurityTool.convertNamespacePermissions(perms), ZooUtil.NodeExistsPolicy.FAIL);
    }
  }

  public void initUser(ZooReaderWriter zoo, String user) throws AccumuloSecurityException {
    try {
      zoo.putPersistentData(ZKUserPath + "/" + user, new byte[0], ZooUtil.NodeExistsPolicy.SKIP);
      zoo.putPersistentData(ZKUserPath + "/" + user + ZKUserTablePerms, new byte[0],
          ZooUtil.NodeExistsPolicy.SKIP);
      zoo.putPersistentData(ZKUserPath + "/" + user + ZKUserNamespacePerms, new byte[0],
          ZooUtil.NodeExistsPolicy.SKIP);
    } catch (KeeperException e) {
      log.error("{}", e.getMessage(), e);
      throw new AccumuloSecurityException(user, SecurityErrorCode.CONNECTION_ERROR, e);
    } catch (InterruptedException e) {
      log.error("{}", e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public Auth auth() {
    return this.auth;
  }

  @Override
  public Perm perm() {
    return this.perm;
  }
}
