package org.apache.accumulo.server.security.handler;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthImpl implements Auth {
  private static final Logger log = LoggerFactory.getLogger(AuthImpl.class);
  private ZooCache zooCache;
  private ServerContext context;
  private String ZKUserPath;

  public AuthImpl(ZooCache zooCache, ServerContext context, String ZKUserPath) {
    this.zooCache = zooCache;
    this.context = context;
    this.ZKUserPath = ZKUserPath;
  }

  @Override
  public boolean authenticate(String userPrincipal, AuthenticationToken token)
      throws AccumuloSecurityException {
    return false;
  }

  @Override
  public Authorizations getAuthorizations(String userPrincipal) {
    return null;
  }

  @Override
  public boolean hasAuths(String user, Authorizations authorizations) {
    return false;
  }

  @Override
  public void changeAuthorizations(String principal, Authorizations authorizations)
      throws AccumuloSecurityException {

  }

  @Override
  public void changePassword(String principal, AuthenticationToken token)
      throws AccumuloSecurityException {

  }

  @Override
  public void createUser(String principal, AuthenticationToken token)
      throws AccumuloSecurityException {
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
      IZooReaderWriter zoo = context.getZooReaderWriter();
      zoo.putPrivatePersistentData(ZKUserPath + "/" + user, pass, ZooUtil.NodeExistsPolicy.FAIL);
    }
  }

  @Override
  public void dropUser(String principal) throws AccumuloSecurityException {

  }
}
