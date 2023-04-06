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
package org.apache.accumulo.cluster;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import java.io.File;
import java.io.IOException;

import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Simple wrapper around a principal and its credentials: a password or a keytab.
 */
public class ClusterUser {
  private String password;
  private String principal;
  private File keytab;

  public ClusterUser(String principal, File keytab) {
    requireNonNull(principal, "Principal was null");
    requireNonNull(keytab, "Keytab was null");
    checkArgument(keytab.exists() && keytab.isFile(), "Keytab should be a file");
    this.principal = principal;
    this.keytab = keytab;
  }

  public ClusterUser(String principal, String password) {
    requireNonNull(principal, "Principal was null");
    requireNonNull(password, "Password was null");
    this.principal = principal;
    this.password = password;
  }

  /**
   * @return the principal
   */
  public String getPrincipal() {
    return principal;
  }

  /**
   * @return the keytab, or null if login is password-based
   */
  public File getKeytab() {
    return keytab;
  }

  /**
   * @return the password, or null if login is keytab-based
   */
  public String getPassword() {
    return password;
  }

  /**
   * Computes the appropriate {@link AuthenticationToken} for the user represented by this object.
   * May not yet be created in Accumulo.
   *
   * @return the correct {@link AuthenticationToken} to use with Accumulo for this user
   * @throws IOException if performing necessary login failed
   */
  public AuthenticationToken getToken() throws IOException {
    if (password != null) {
      return new PasswordToken(password);
    } else if (keytab != null) {
      UserGroupInformation.loginUserFromKeytab(principal, keytab.getAbsolutePath());
      return new KerberosToken();
    }

    throw new IllegalStateException("One of password and keytab must be non-null");
  }

  @Override
  public String toString() {
    return "KerberosPrincipal [principal=" + principal + ", keytab=" + keytab + ", password="
        + password + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + principal.hashCode();
    result = prime * result + (keytab == null ? 0 : keytab.hashCode());
    result = prime * result + (password == null ? 0 : password.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj == null) {
      return false;
    }

    if (obj instanceof ClusterUser) {
      ClusterUser other = (ClusterUser) obj;
      if (keytab == null) {
        if (other.keytab != null) {
          return false;
        }
      } else if (!keytab.equals(other.keytab)) {
        return false;
      }

      if (password == null) {
        if (other.password != null) {
          return false;
        }
      } else if (!password.equals(other.password)) {
        return false;
      }

      return principal.equals(other.principal);
    }

    return false;
  }

}
