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
package org.apache.accumulo.core.client.security.tokens;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import javax.security.auth.DestroyFailedException;

import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.base.Preconditions;

/**
 * Authentication token for Kerberos authenticated clients
 *
 * @since 1.7.0
 */
public class KerberosToken implements AuthenticationToken {

  public static final String CLASS_NAME = KerberosToken.class.getName();

  private static final int VERSION = 1;

  private String principal;
  private File keytab;

  /**
   * Creates a token using the provided principal and the currently logged-in user via {@link UserGroupInformation}.
   *
   * @param principal
   *          The user that is logged in
   */
  public KerberosToken(String principal) throws IOException {
    Preconditions.checkNotNull(principal);
    final UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    Preconditions.checkArgument(ugi.hasKerberosCredentials(), "Subject is not logged in via Kerberos");
    Preconditions.checkArgument(principal.equals(ugi.getUserName()), "Provided principal does not match currently logged-in user");
    this.principal = ugi.getUserName();
  }

  /**
   * Creates a token and logs in via {@link UserGroupInformation} using the provided principal and keytab. A key for the principal must exist in the keytab,
   * otherwise login will fail.
   *
   * @param principal
   *          The Kerberos principal
   * @param keytab
   *          A keytab file
   * @param replaceCurrentUser
   *          Should the current Hadoop user be replaced with this user
   */
  public KerberosToken(String principal, File keytab, boolean replaceCurrentUser) throws IOException {
    Preconditions.checkNotNull(principal, "Principal was null");
    Preconditions.checkNotNull(keytab, "Keytab was null");
    Preconditions.checkArgument(keytab.exists() && keytab.isFile(), "Keytab was not a normal file");
    UserGroupInformation ugi;
    if (replaceCurrentUser) {
      UserGroupInformation.loginUserFromKeytab(principal, keytab.getAbsolutePath());
      ugi = UserGroupInformation.getCurrentUser();
    } else {
      ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab.getAbsolutePath());
    }
    this.principal = ugi.getUserName();
    this.keytab = keytab;
  }

  /**
   * Creates a token using the login user as returned by {@link UserGroupInformation#getCurrentUser()}
   *
   * @throws IOException
   *           If the current logged in user cannot be computed.
   */
  public KerberosToken() throws IOException {
    this(UserGroupInformation.getCurrentUser().getUserName());
  }

  @Override
  public KerberosToken clone() {
    try {
      KerberosToken clone = (KerberosToken) super.clone();
      clone.principal = principal;
      clone.keytab = keytab == null ? keytab : keytab.getCanonicalFile();
      return clone;
    } catch (CloneNotSupportedException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof KerberosToken))
      return false;
    KerberosToken other = (KerberosToken) obj;

    return principal.equals(other.principal);
  }

  /**
   * The identity of the user to which this token belongs to according to Kerberos
   *
   * @return The principal
   */
  public String getPrincipal() {
    return principal;
  }

  /**
   * The keytab file used to perform Kerberos login. Optional, may be null.
   */
  public File getKeytab() {
    return keytab;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(VERSION);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int actualVersion = in.readInt();
    if (VERSION != actualVersion) {
      throw new IOException("Did not find expected version in serialized KerberosToken");
    }
  }

  @Override
  public synchronized void destroy() throws DestroyFailedException {
    principal = null;
  }

  @Override
  public boolean isDestroyed() {
    return null == principal;
  }

  @Override
  public void init(Properties properties) {

  }

  @Override
  public Set<TokenProperty> getProperties() {
    return Collections.emptySet();
  }

  @Override
  public int hashCode() {
    return principal.hashCode();
  }
}
