package org.apache.accumulo.server.security.handler;

import static org.apache.accumulo.server.security.handler.ZKSecurityModuleImpl.ZKUserAuths;
import static org.apache.accumulo.server.security.handler.ZKSecurityModuleImpl.ZKUserNamespacePerms;
import static org.apache.accumulo.server.security.handler.ZKSecurityModuleImpl.ZKUserSysPerms;
import static org.apache.accumulo.server.security.handler.ZKSecurityModuleImpl.ZKUserTablePerms;

import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.security.Auth;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ZKAuthImpl implements Auth {

  private final Logger log = LoggerFactory.getLogger(ZKAuthImpl.class);

  final ServerContext context;
  final ZooCache zooCache;
  final String ZKUserPath;

  public ZKAuthImpl(ServerContext context, ZooCache zooCache, String zkuserPath) {
    this.context = context;
    this.zooCache = zooCache;
    this.ZKUserPath = zkuserPath;
  }

  @Override
  public boolean authenticate(String principal, AuthenticationToken token)
          throws AccumuloSecurityException {
    if (!(token instanceof PasswordToken))
      throw new AccumuloSecurityException(principal, SecurityErrorCode.INVALID_TOKEN);
    PasswordToken pt = (PasswordToken) token;
    byte[] pass;
    String zpath = ZKUserPath + "/" + principal;
    pass = zooCache.get(zpath);
    boolean result = ZKSecurityTool.checkPass(pt.getPassword(), pass);
    if (!result) {
      zooCache.clear(zpath);
      pass = zooCache.get(zpath);
      result = ZKSecurityTool.checkPass(pt.getPassword(), pass);
    }
    return result;
  }

  @Override
  public Authorizations getAuths(String user) {
    byte[] authsBytes = zooCache.get(ZKUserPath + "/" + user + ZKUserAuths);
    if (authsBytes != null)
      return ZKSecurityTool.convertAuthorizations(authsBytes);
    return Authorizations.EMPTY;
  }

  public boolean hasAuths(String user, Authorizations auths) {
    if (auths.isEmpty()) {
      // avoid deserializing auths from ZK cache
      return true;
    }

    Authorizations userauths = getAuths(user);

    for (byte[] auth : auths.getAuthorizations()) {
      if (!userauths.contains(auth)) {
        return false;
      }
    }

    return true;
  }

  @Override
  public void setAuths(String user, Authorizations authorizations)
          throws AccumuloSecurityException {
    try {
      synchronized (zooCache) {
        zooCache.clear();
        context.getZooReaderWriter().putPersistentData(ZKUserPath + "/" + user + ZKUserAuths,
                ZKSecurityTool.convertAuthorizations(authorizations),
                ZooUtil.NodeExistsPolicy.OVERWRITE);
      }
    } catch (KeeperException e) {
      log.error("{}", e.getMessage(), e);
      throw new AccumuloSecurityException(user, SecurityErrorCode.CONNECTION_ERROR, e);
    } catch (InterruptedException e) {
      log.error("{}", e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void changePassword(String principal, AuthenticationToken token)
          throws AccumuloSecurityException {
    if (!(token instanceof PasswordToken))
      throw new AccumuloSecurityException(principal, SecurityErrorCode.INVALID_TOKEN);
    PasswordToken pt = (PasswordToken) token;
    if (userExists(principal)) {
      try {
        synchronized (zooCache) {
          zooCache.clear(ZKUserPath + "/" + principal);
          context.getZooReaderWriter().putPrivatePersistentData(ZKUserPath + "/" + principal,
                  ZKSecurityTool.createPass(pt.getPassword()), ZooUtil.NodeExistsPolicy.OVERWRITE);
        }
      } catch (KeeperException e) {
        log.error("{}", e.getMessage(), e);
        throw new AccumuloSecurityException(principal, SecurityErrorCode.CONNECTION_ERROR, e);
      } catch (InterruptedException e) {
        log.error("{}", e.getMessage(), e);
        throw new RuntimeException(e);
      } catch (AccumuloException e) {
        log.error("{}", e.getMessage(), e);
        throw new AccumuloSecurityException(principal, SecurityErrorCode.DEFAULT_SECURITY_ERROR, e);
      }
    } else
      // user doesn't exist
      throw new AccumuloSecurityException(principal, SecurityErrorCode.USER_DOESNT_EXIST);
  }

  /**
   *
   */
  void initUser(String user) throws AccumuloSecurityException {
    // Copied and merged from Authorizer.initUser(user) and PermissionHandler.initUser()
    ZooReaderWriter zoo = context.getZooReaderWriter();
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
  public void createUser(String principal, AuthenticationToken token)
          throws AccumuloSecurityException {
    initUser(principal);

    try {
      if (!(token instanceof PasswordToken))
        throw new AccumuloSecurityException(principal, SecurityErrorCode.INVALID_TOKEN);
      PasswordToken pt = (PasswordToken) token;
      constructUser(principal, ZKSecurityTool.createPass(pt.getPassword()));
    } catch (KeeperException e) {
      if (e.code().equals(KeeperException.Code.NODEEXISTS))
        throw new AccumuloSecurityException(principal, SecurityErrorCode.USER_EXISTS, e);
      throw new AccumuloSecurityException(principal, SecurityErrorCode.CONNECTION_ERROR, e);
    } catch (InterruptedException e) {
      log.error("{}", e.getMessage(), e);
      throw new RuntimeException(e);
    } catch (AccumuloException e) {
      log.error("{}", e.getMessage(), e);
      throw new AccumuloSecurityException(principal, SecurityErrorCode.DEFAULT_SECURITY_ERROR, e);
    }
  }

  /**
   * Sets up the user in ZK for the provided user. No checking for existence is done here, it should
   * be done before calling.
   */
  public void constructUser(String user, byte[] pass) throws KeeperException, InterruptedException {
    synchronized (zooCache) {
      zooCache.clear();
      ZooReaderWriter zoo = context.getZooReaderWriter();
      zoo.putPrivatePersistentData(ZKUserPath + "/" + user, pass, ZooUtil.NodeExistsPolicy.FAIL);
    }
  }

  @Override
  public void dropUser(String user) throws AccumuloSecurityException {
    try {
      synchronized (zooCache) {
        zooCache.clear();
        context.getZooReaderWriter().recursiveDelete(ZKUserPath + "/" + user,
                ZooUtil.NodeMissingPolicy.FAIL);
      }
    } catch (InterruptedException e) {
      log.error("{}", e.getMessage(), e);
      throw new RuntimeException(e);
    } catch (KeeperException e) {
      if (e.code().equals(KeeperException.Code.NONODE)) {
        throw new AccumuloSecurityException(user, SecurityErrorCode.USER_DOESNT_EXIST, e);
      }
      log.error("{}", e.getMessage(), e);
      throw new AccumuloSecurityException(user, SecurityErrorCode.CONNECTION_ERROR, e);
    }

    cleanUser(user);
  }

  /**
   * Copied from ZKPermHandler.cleanUser()
   */
  private void cleanUser(String user) throws AccumuloSecurityException {
    ZooReaderWriter zoo = context.getZooReaderWriter();

    try {
      synchronized (zooCache) {
        zoo.recursiveDelete(ZKUserPath + "/" + user + ZKUserSysPerms,
                ZooUtil.NodeMissingPolicy.SKIP);
        zoo.recursiveDelete(ZKUserPath + "/" + user + ZKUserTablePerms,
                ZooUtil.NodeMissingPolicy.SKIP);
        zoo.recursiveDelete(ZKUserPath + "/" + user + ZKUserNamespacePerms,
                ZooUtil.NodeMissingPolicy.SKIP);
        zooCache.clear(ZKUserPath + "/" + user);
      }
    } catch (InterruptedException e) {
      log.error("{}", e.getMessage(), e);
      throw new RuntimeException(e);
    } catch (KeeperException e) {
      log.error("{}", e.getMessage(), e);
      if (e.code().equals(KeeperException.Code.NONODE))
        throw new AccumuloSecurityException(user, SecurityErrorCode.USER_DOESNT_EXIST, e);
      throw new AccumuloSecurityException(user, SecurityErrorCode.CONNECTION_ERROR, e);

    }
  }

  private boolean userExists(String user) {
    return zooCache.get(ZKUserPath + "/" + user) != null;
  }

  @Override
  public Set<String> listUsers() {
    return new TreeSet<>(zooCache.getChildren(ZKUserPath));
  }

  // ZK stuff

}
