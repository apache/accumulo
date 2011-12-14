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

import org.apache.accumulo.core.security.thrift.SecurityErrorCode;
import org.apache.accumulo.core.security.thrift.ThriftSecurityException;

/**
 * An Accumulo Exception for security violations, authentication failures, authorization failures, etc.
 * 
 */
public class AccumuloSecurityException extends Exception {
  private static final long serialVersionUID = 1L;
  
  private static String getDefaultErrorMessage(SecurityErrorCode errorcode) {
    switch (errorcode) {
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
      case DEFAULT_SECURITY_ERROR:
      default:
        return "Unknown security exception";
    }
  }
  
  private String user;
  private SecurityErrorCode errorCode;
  
  /**
   * @return this exception as a thrift exception
   */
  public ThriftSecurityException asThriftException() {
    return new ThriftSecurityException(user, errorCode);
  }
  
  /**
   * @param user
   *          the relevant user for the security violation
   * @param errorcode
   *          the specific reason for this exception
   * @param cause
   *          the exception that caused this violation
   */
  public AccumuloSecurityException(String user, SecurityErrorCode errorcode, Throwable cause) {
    super(getDefaultErrorMessage(errorcode), cause);
    this.user = user;
    this.errorCode = errorcode == null ? SecurityErrorCode.DEFAULT_SECURITY_ERROR : errorcode;
  }
  
  /**
   * @param user
   *          the relevant user for the security violation
   * @param errorcode
   *          the specific reason for this exception
   */
  public AccumuloSecurityException(String user, SecurityErrorCode errorcode) {
    super(getDefaultErrorMessage(errorcode));
    this.user = user;
    this.errorCode = errorcode == null ? SecurityErrorCode.DEFAULT_SECURITY_ERROR : errorcode;
  }
  
  /**
   * @return the relevant user for the security violation
   */
  public String getUser() {
    return user;
  }
  
  /**
   * @return the specific reason for this exception
   */
  public SecurityErrorCode getErrorCode() {
    return errorCode;
  }
  
  public String getMessage() {
    return "Error " + errorCode + " - " + super.getMessage();
  }
}
