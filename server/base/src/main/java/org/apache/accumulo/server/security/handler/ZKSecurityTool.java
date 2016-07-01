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
package org.apache.accumulo.server.security.handler;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * All the static too methods used for this class, so that we can separate out stuff that isn't using ZooKeeper. That way, we can check the synchronization
 * model more easily, as we only need to check to make sure zooCache is cleared when things are written to ZooKeeper in methods that might use it. These won't,
 * and so don't need to be checked.
 */
class ZKSecurityTool {
  private static final Logger log = LoggerFactory.getLogger(ZKSecurityTool.class);
  private static final int SALT_LENGTH = 8;

  // Generates a byte array salt of length SALT_LENGTH
  private static byte[] generateSalt() {
    final SecureRandom random = new SecureRandom();
    byte[] salt = new byte[SALT_LENGTH];
    random.nextBytes(salt);
    return salt;
  }

  private static byte[] hash(byte[] raw) throws NoSuchAlgorithmException {
    MessageDigest md = MessageDigest.getInstance(Constants.PW_HASH_ALGORITHM);
    md.update(raw);
    return md.digest();
  }

  public static boolean checkPass(byte[] password, byte[] zkData) {
    if (zkData == null)
      return false;

    byte[] salt = new byte[SALT_LENGTH];
    System.arraycopy(zkData, 0, salt, 0, SALT_LENGTH);
    byte[] passwordToCheck;
    try {
      passwordToCheck = convertPass(password, salt);
    } catch (NoSuchAlgorithmException e) {
      log.error("Count not create hashed password", e);
      return false;
    }
    return java.util.Arrays.equals(passwordToCheck, zkData);
  }

  public static byte[] createPass(byte[] password) throws AccumuloException {
    byte[] salt = generateSalt();
    try {
      return convertPass(password, salt);
    } catch (NoSuchAlgorithmException e) {
      log.error("Count not create hashed password", e);
      throw new AccumuloException("Count not create hashed password", e);
    }
  }

  private static byte[] convertPass(byte[] password, byte[] salt) throws NoSuchAlgorithmException {
    byte[] plainSalt = new byte[password.length + SALT_LENGTH];
    System.arraycopy(password, 0, plainSalt, 0, password.length);
    System.arraycopy(salt, 0, plainSalt, password.length, SALT_LENGTH);
    byte[] hashed = hash(plainSalt);
    byte[] saltedHash = new byte[SALT_LENGTH + hashed.length];
    System.arraycopy(salt, 0, saltedHash, 0, SALT_LENGTH);
    System.arraycopy(hashed, 0, saltedHash, SALT_LENGTH, hashed.length);
    return saltedHash; // contains salt+hash(password+salt)
  }

  public static Authorizations convertAuthorizations(byte[] authorizations) {
    return new Authorizations(authorizations);
  }

  public static byte[] convertAuthorizations(Authorizations authorizations) {
    return authorizations.getAuthorizationsArray();
  }

  public static byte[] convertSystemPermissions(Set<SystemPermission> systempermissions) {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream(systempermissions.size());
    DataOutputStream out = new DataOutputStream(bytes);
    try {
      for (SystemPermission sp : systempermissions)
        out.writeByte(sp.getId());
    } catch (IOException e) {
      log.error("{}", e.getMessage(), e);
      throw new RuntimeException(e); // this is impossible with ByteArrayOutputStream; crash hard if this happens
    }
    return bytes.toByteArray();
  }

  public static Set<SystemPermission> convertSystemPermissions(byte[] systempermissions) {
    ByteArrayInputStream bytes = new ByteArrayInputStream(systempermissions);
    DataInputStream in = new DataInputStream(bytes);
    Set<SystemPermission> toReturn = new HashSet<>();
    try {
      while (in.available() > 0)
        toReturn.add(SystemPermission.getPermissionById(in.readByte()));
    } catch (IOException e) {
      log.error("User database is corrupt; error converting system permissions", e);
      toReturn.clear();
    }
    return toReturn;
  }

  public static byte[] convertTablePermissions(Set<TablePermission> tablepermissions) {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream(tablepermissions.size());
    DataOutputStream out = new DataOutputStream(bytes);
    try {
      for (TablePermission tp : tablepermissions)
        out.writeByte(tp.getId());
    } catch (IOException e) {
      log.error("{}", e.getMessage(), e);
      throw new RuntimeException(e); // this is impossible with ByteArrayOutputStream; crash hard if this happens
    }
    return bytes.toByteArray();
  }

  public static Set<TablePermission> convertTablePermissions(byte[] tablepermissions) {
    Set<TablePermission> toReturn = new HashSet<>();
    for (byte b : tablepermissions)
      toReturn.add(TablePermission.getPermissionById(b));
    return toReturn;
  }

  public static byte[] convertNamespacePermissions(Set<NamespacePermission> namespacepermissions) {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream(namespacepermissions.size());
    DataOutputStream out = new DataOutputStream(bytes);
    try {
      for (NamespacePermission tnp : namespacepermissions)
        out.writeByte(tnp.getId());
    } catch (IOException e) {
      log.error("{}", e.getMessage(), e);
      throw new RuntimeException(e); // this is impossible with ByteArrayOutputStream; crash hard if this happens
    }
    return bytes.toByteArray();
  }

  public static Set<NamespacePermission> convertNamespacePermissions(byte[] namespacepermissions) {
    Set<NamespacePermission> toReturn = new HashSet<>();
    for (byte b : namespacepermissions)
      toReturn.add(NamespacePermission.getPermissionById(b));
    return toReturn;
  }

  public static String getInstancePath(String instanceId) {
    return Constants.ZROOT + "/" + instanceId;
  }
}
