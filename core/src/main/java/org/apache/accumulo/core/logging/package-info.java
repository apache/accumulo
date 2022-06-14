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
/**
 * This package exist to provide a central place in Accumulo's source code for important log
 * messages. This allows Accumulo's users to look in one place in the source code to see whats
 * available. It also allows user to diff this directory across Accumulo releases to see what has
 * changed, which enable them to update automated parsing.
 *
 * <p>
 * The log messages in this package exist within a logical namespace independently from source code
 * package names. This convention allows user to track logical events like changes to tablet files
 * where the code making those changes may exist in many different packages and classes.
 *
 * <p>
 * Since users may have automation for parsing log messages, please be mindful of this when making
 * changes to log messages in bug fix releases.
 */
package org.apache.accumulo.core.logging;
