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
package org.apache.accumulo.core.client.security.tokens;

import org.apache.accumulo.core.client.admin.SecurityOperations;

/**
 * An {@link AuthenticationToken} that wraps a "Hadoop style" delegation token created by Accumulo.
 * The intended scope of this token is when a KerberosToken cannot be used instead. The most common
 * reason for this is within YARN jobs. The Kerberos credentials of the user are not passed over the
 * wire to the job itself. The delegation token serves as a mechanism to obtain a transient shared
 * secret with Accumulo using a {@link KerberosToken} and then run some task authenticating with
 * that shared secret.
 *
 * <p>
 * Obtain a delegation token by calling
 * {@link SecurityOperations#getDelegationToken(org.apache.accumulo.core.client.admin.DelegationTokenConfig)}
 *
 * @since 1.7.0
 */
public interface DelegationToken extends AuthenticationToken {

}
