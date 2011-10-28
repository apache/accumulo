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
package org.apache.accumulo.server.client;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.impl.HdfsZooInstance;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.thrift.ClientService;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.client.impl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.map.MyMapFile;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.security.thrift.SecurityErrorCode;
import org.apache.accumulo.core.security.thrift.ThriftSecurityException;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.server.security.Authenticator;
import org.apache.accumulo.server.security.ZKAuthenticator;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import cloudtrace.thrift.TInfo;

@SuppressWarnings("deprecation")
public class ClientServiceHandler implements ClientService.Iface {
  private static final Logger log = Logger.getLogger(ClientServiceHandler.class);
  private static Authenticator authenticator = ZKAuthenticator.getInstance();
  
  protected String checkTableId(String tableName, TableOperation operation) throws ThriftTableOperationException {
    final String tableId = Tables.getNameToIdMap(HdfsZooInstance.getInstance()).get(tableName);
    if (tableId == null)
      throw new ThriftTableOperationException(null, tableName, operation, TableOperationExceptionType.NOTFOUND, null);
    return tableId;
  }
  
  @Override
  public String getInstanceId() {
    return HdfsZooInstance.getInstance().getInstanceID();
  }
  
  @Override
  public String getRootTabletLocation() {
    return HdfsZooInstance.getInstance().getRootTabletLocation();
  }
  
  @Override
  public String getZooKeepers() {
    return AccumuloConfiguration.getSystemConfiguration().get(Property.INSTANCE_ZK_HOST);
  }
  
  @Override
  public void ping(AuthInfo credentials) {
    // anybody can call this; no authentication check
    log.info("Master reports: I just got pinged!");
  }
  
  @Override
  public boolean authenticateUser(TInfo tinfo, AuthInfo credentials, String user, byte[] password) throws ThriftSecurityException {
    try {
      return authenticator.authenticateUser(credentials, user, password);
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }
  
  @Override
  public void changeAuthorizations(TInfo tinfo, AuthInfo credentials, String user, List<byte[]> authorizations) throws ThriftSecurityException {
    try {
      authenticator.changeAuthorizations(credentials, user, new Authorizations(authorizations));
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }
  
  @Override
  public void changePassword(TInfo tinfo, AuthInfo credentials, String user, byte[] password) throws ThriftSecurityException {
    try {
      authenticator.changePassword(credentials, user, password);
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }
  
  @Override
  public void createUser(TInfo tinfo, AuthInfo credentials, String user, byte[] password, List<byte[]> authorizations) throws ThriftSecurityException {
    try {
      authenticator.createUser(credentials, user, password, new Authorizations(authorizations));
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }
  
  @Override
  public void dropUser(TInfo tinfo, AuthInfo credentials, String user) throws ThriftSecurityException {
    try {
      authenticator.dropUser(credentials, user);
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }
  
  @Override
  public List<byte[]> getUserAuthorizations(TInfo tinfo, AuthInfo credentials, String user) throws ThriftSecurityException {
    try {
      return authenticator.getUserAuthorizations(credentials, user).getAuthorizations();
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }
  
  @Override
  public void grantSystemPermission(TInfo tinfo, AuthInfo credentials, String user, byte permission) throws ThriftSecurityException {
    try {
      authenticator.grantSystemPermission(credentials, user, SystemPermission.getPermissionById(permission));
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }
  
  @Override
  public void grantTablePermission(TInfo tinfo, AuthInfo credentials, String user, String tableName, byte permission) throws ThriftSecurityException,
      ThriftTableOperationException {
    String tableId = checkTableId(tableName, TableOperation.PERMISSION);
    try {
      authenticator.grantTablePermission(credentials, user, tableId, TablePermission.getPermissionById(permission));
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }
  
  @Override
  public void revokeSystemPermission(TInfo tinfo, AuthInfo credentials, String user, byte permission) throws ThriftSecurityException {
    try {
      authenticator.revokeSystemPermission(credentials, user, SystemPermission.getPermissionById(permission));
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }
  
  @Override
  public void revokeTablePermission(TInfo tinfo, AuthInfo credentials, String user, String tableName, byte permission) throws ThriftSecurityException,
      ThriftTableOperationException {
    String tableId = checkTableId(tableName, TableOperation.PERMISSION);
    try {
      authenticator.revokeTablePermission(credentials, user, tableId, TablePermission.getPermissionById(permission));
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }
  
  @Override
  public boolean hasSystemPermission(TInfo tinfo, AuthInfo credentials, String user, byte sysPerm) throws ThriftSecurityException {
    try {
      return authenticator.hasSystemPermission(credentials, user, SystemPermission.getPermissionById(sysPerm));
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }
  
  @Override
  public boolean hasTablePermission(TInfo tinfo, AuthInfo credentials, String user, String tableName, byte tblPerm) throws ThriftSecurityException,
      ThriftTableOperationException {
    String tableId = checkTableId(tableName, TableOperation.PERMISSION);
    try {
      return authenticator.hasTablePermission(credentials, user, tableId, TablePermission.getPermissionById(tblPerm));
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }
  
  @Override
  public Set<String> listUsers(TInfo tinfo, AuthInfo credentials) throws ThriftSecurityException {
    try {
      return authenticator.listUsers(credentials);
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }
  
  private Path createNewBulkDir(FileSystem fs, String tableId) throws IOException {
    Path directory = new Path(Constants.getTablesDir() + "/" + tableId);
    fs.mkdirs(directory);
    
    // only one should be able to create the lock file
    // the purpose of the lock file is to avoid a race
    // condition between the call to fs.exists() and
    // fs.mkdirs()... if only hadoop had a mkdir() function
    // that failed when the dir existed
    while (true) {
      String uuid = UUID.randomUUID().toString();
      Path lockPath = new Path(directory, "l_" + uuid);
      if (fs.createNewFile(lockPath)) {
        try {
          Path newBulkDir = new Path(directory, "bulk_" + uuid);
          if (!fs.exists(newBulkDir)) {
            if (fs.mkdirs(newBulkDir))
              return newBulkDir;
            log.warn("Failed to create " + newBulkDir + " for unknown reason");
          } else {
            // name collision
            log.warn("Unlikely event occurred, name collision uuid = " + uuid);
          }
        } finally {
          try {
            fs.delete(lockPath, false);
          } catch (IOException e) {
            log.warn("Failed to delete lock path " + lockPath + " " + e.getMessage());
          }
        }
      } else {
        log.warn("Unlikely event occurred, name collision uuid = " + uuid);
      }
      UtilWaitThread.sleep(3000);
    }
  }
  
  @Override
  public String prepareBulkImport(TInfo info, AuthInfo credentials, String dir, String tableName, double errPercent) throws ThriftSecurityException,
      ThriftTableOperationException, TException {
    String tableId = checkTableId(tableName, TableOperation.PERMISSION);
    try {
      if (!authenticator.hasTablePermission(credentials, credentials.getUser(), tableId, TablePermission.BULK_IMPORT))
        throw new AccumuloSecurityException(credentials.getUser(), SecurityErrorCode.PERMISSION_DENIED);
      FileSystem fs = FileSystem.get(CachedConfiguration.getInstance());
      Path bulkDir = createNewBulkDir(fs, tableId);
      Path procFile = new Path(bulkDir.toString() + "/processing_proc_" + System.currentTimeMillis());
      FSDataOutputStream fsOut = fs.create(procFile);
      fsOut.write(String.valueOf(System.currentTimeMillis()).getBytes());
      fsOut.close();
      Path dirPath = new Path(dir);
      FileStatus[] mapFiles = fs.listStatus(dirPath);
      
      int seq = 0;
      int errorCount = 0;
      
      for (FileStatus fileStatus : mapFiles) {
        if ((double) errorCount / (double) mapFiles.length > errPercent)
          throw new IOException(String.format("More than %6.2f%s of map files failed to move", errPercent * 100.0, "%"));
        
        String sa[] = fileStatus.getPath().getName().split("\\.");
        String extension = "";
        if (sa.length > 1) {
          extension = sa[sa.length - 1];
          
          if (!FileOperations.getValidExtensions().contains(extension)) {
            log.warn(fileStatus.getPath() + " does not have a valid extension, ignoring");
            continue;
          }
        } else {
          // assume it is a map file
          extension = MyMapFile.EXTENSION;
        }
        
        if (extension.equals(MyMapFile.EXTENSION)) {
          if (!fileStatus.isDir()) {
            log.warn(fileStatus.getPath() + " is not a map file, ignoring");
            continue;
          }
          
          try {
            FileStatus dataStatus = fs.getFileStatus(new Path(fileStatus.getPath(), MyMapFile.DATA_FILE_NAME));
            if (dataStatus.isDir()) {
              log.warn(fileStatus.getPath() + " is not a map file, ignoring");
              continue;
            }
          } catch (FileNotFoundException fnfe) {
            log.warn(fileStatus.getPath() + " is not a map file, ignoring");
            continue;
          }
        }
        
        String newName = String.format("%05d", 0) + "_" + String.format("%05d", seq++) + "." + extension;
        Path newPath = new Path(bulkDir, newName);
        try {
          fs.rename(fileStatus.getPath(), newPath);
          log.debug("Moved " + fileStatus.getPath() + " to " + newPath);
        } catch (IOException E1) {
          log.error("Could not move: " + fileStatus.getPath().toString() + " " + E1.getMessage());
          errorCount += 1;
        }
      }
      return procFile.toString();
    } catch (AccumuloSecurityException ex) {
      throw ex.asThriftException();
    } catch (Exception ex) {
      log.error("Error preparing bulk import directory " + dir, ex);
      return null;
    }
  }
  
  @Override
  public void finishBulkImport(TInfo tinfo, AuthInfo credentials, String tableName, String lockFile, boolean disableGC) throws ThriftSecurityException,
      ThriftTableOperationException, TException {
    String tableId = checkTableId(tableName, TableOperation.PERMISSION);
    try {
      // make sure the user can still do bulk import
      if (!authenticator.hasTablePermission(credentials, credentials.getUser(), tableId, TablePermission.BULK_IMPORT))
        throw new AccumuloSecurityException(credentials.getUser(), SecurityErrorCode.PERMISSION_DENIED);
      Path lockPath = new Path(lockFile);
      Path bulkDir = lockPath.getParent();
      // ensure the table matches
      if (!bulkDir.getParent().getName().equals(tableId))
        throw new AccumuloSecurityException(credentials.getUser(), SecurityErrorCode.PERMISSION_DENIED);
      // ensure the location is in the table directory
      if (!bulkDir.getParent().getParent().toString().equals(Constants.getTablesDir()))
        throw new AccumuloSecurityException(credentials.getUser(), SecurityErrorCode.PERMISSION_DENIED);
      FileSystem fs = FileSystem.get(CachedConfiguration.getInstance());
      
      if (disableGC) {
        Path disableFlagFile = new Path(bulkDir.getParent(), "processing_proc_disableGC");
        log.debug("Creating file " + disableFlagFile);
        FSDataOutputStream fsOut2 = fs.create(disableFlagFile);
        fsOut2.write(String.valueOf(System.currentTimeMillis()).getBytes());
        fsOut2.close();
      }
      
      if (lockPath.getName().startsWith("processing_proc_"))
        fs.delete(lockPath, false);
      
    } catch (AccumuloSecurityException ex) {
      throw ex.asThriftException();
    } catch (Exception ex) {
      log.error("Error removing bulk import processing file", ex);
    }
  }
  
}
