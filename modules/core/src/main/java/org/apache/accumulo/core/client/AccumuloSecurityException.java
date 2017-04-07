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
package org.apache.accumulo.core.client;

import org.apache.accumulo.core.client.impl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.commons.lang.StringUtils;

/**
 * An Accumulo Exception for security violations, authentication failures, authorization failures, etc.
 *
 */
public class AccumuloSecurityException extends Exception {
  private static final long serialVersionUID = 1L;

  private static String getDefaultErrorMessage(final SecurityErrorCode errorcode) {
    switch (errorcode == null ? SecurityErrorCode.DEFAULT_SECURITY_ERROR : errorcode) {
      case BAD_CREDENTIALS:
        return "Username or Password is Invalid";
      case CONNECTION_ERROR:
        return "Connection Error Occurred";
      case PERMISSION_DENIED:
        return "User does not have permission to perform this action";
      case USER_DOESNT_EXIST:
        return "The user does not exist";
      case USER_EXISTS:
        return "The user exists";
      case GRANT_INVALID:
        return "The GRANT permission cannot be granted or revoked";
      case BAD_AUTHORIZATIONS:
        return "The user does not have the specified authorizations assigned";
      case UNSUPPORTED_OPERATION:
        return "The configured security handler does not support this operation";
      case INVALID_TOKEN:
        return "The configured authenticator does not accept this type of token";
      case AUTHENTICATOR_FAILED:
        return "The configured authenticator failed for some reason";
      case AUTHORIZOR_FAILED:
        return "The configured authorizor failed for some reason";
      case PERMISSIONHANDLER_FAILED:
        return "The configured permission handler failed for some reason";
      case TOKEN_EXPIRED:
        return "The supplied token expired, please update and try again";
      case INSUFFICIENT_PROPERTIES:
        return "The login properties supplied are not sufficient for authentication. Please check the requested properties and try again";
      case DEFAULT_SECURITY_ERROR:
      default:
        return "Unknown security exception";
    }
  }

  private String user;
  private String tableInfo;
  private SecurityErrorCode errorCode;

  /**
   * @return this exception as a thrift exception
   */
  public ThriftSecurityException asThriftException() {
    return new ThriftSecurityException(user, errorCode);
  }

  /**
   * Construct a user-facing exception from a serialized version.
   *
   * @param thrift
   *          a serialized version
   */
  public AccumuloSecurityException(final ThriftSecurityException thrift) {
    this(thrift.getUser(), thrift.getCode(), thrift);
  }

  /**
   * @param user
   *          the relevant user for the security violation
   * @param errorcode
   *          the specific reason for this exception
   * @param cause
   *          the exception that caused this violation
   */
  public AccumuloSecurityException(final String user, final SecurityErrorCode errorcode, final Throwable cause) {
    super(getDefaultErrorMessage(errorcode), cause);
    this.user = user;
    this.errorCode = errorcode == null ? SecurityErrorCode.DEFAULT_SECURITY_ERROR : errorcode;
  }

  /**
   * @param user
   *          the relevant user for the security violation
   * @param errorcode
   *          the specific reason for this exception
   * @param tableInfo
   *          the relevant tableInfo for the security violation
   * @param cause
   *          the exception that caused this violation
   */
  public AccumuloSecurityException(final String user, final SecurityErrorCode errorcode, final String tableInfo, final Throwable cause) {
    super(getDefaultErrorMessage(errorcode), cause);
    this.user = user;
    this.errorCode = errorcode == null ? SecurityErrorCode.DEFAULT_SECURITY_ERROR : errorcode;
    this.tableInfo = tableInfo;
  }

  /**
   * @param user
   *          the relevant user for the security violation
   * @param errorcode
   *          the specific reason for this exception
   */
  public AccumuloSecurityException(final String user, final SecurityErrorCode errorcode) {
    super(getDefaultErrorMessage(errorcode));
    this.user = user;
    this.errorCode = errorcode == null ? SecurityErrorCode.DEFAULT_SECURITY_ERROR : errorcode;
  }

  /**
   * @param user
   *          the relevant user for the security violation
   * @param errorcode
   *          the specific reason for this exception
   * @param tableInfo
   *          the relevant tableInfo for the security violation
   */
  public AccumuloSecurityException(final String user, final SecurityErrorCode errorcode, final String tableInfo) {
    super(getDefaultErrorMessage(errorcode));
    this.user = user;
    this.errorCode = errorcode == null ? SecurityErrorCode.DEFAULT_SECURITY_ERROR : errorcode;
    this.tableInfo = tableInfo;
  }

  /**
   * @return the relevant user for the security violation
   */
  public String getUser() {
    return user;
  }

  public void setUser(String s) {
    this.user = s;
  }

  /**
   * @return the relevant tableInfo for the security violation
   */
  public String getTableInfo() {
    return tableInfo;
  }

  public void setTableInfo(String tableInfo) {
    this.tableInfo = tableInfo;
  }

  /**
   * @return the specific reason for this exception
   * @since 1.5.0
   */

  public org.apache.accumulo.core.client.security.SecurityErrorCode getSecurityErrorCode() {
    return org.apache.accumulo.core.client.security.SecurityErrorCode.valueOf(errorCode.name());
  }

  @Override
  public String getMessage() {
    StringBuilder message = new StringBuilder();
    message.append("Error ").append(errorCode);
    message.append(" for user ").append(user);
    if (!StringUtils.isEmpty(tableInfo)) {
      message.append(" on table ").append(tableInfo);
    }
    message.append(" - ").append(super.getMessage());
    return message.toString();
  }
}
