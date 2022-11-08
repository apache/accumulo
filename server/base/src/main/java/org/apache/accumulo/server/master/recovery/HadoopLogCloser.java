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
package org.apache.accumulo.server.master.recovery;

import org.apache.accumulo.core.conf.Property;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @deprecated since 2.1.0. Use {@link org.apache.accumulo.server.manager.recovery.HadoopLogCloser}
 *             instead
 */
@Deprecated(since = "2.1.0")
@SuppressFBWarnings(value = "NM_SAME_SIMPLE_NAME_AS_SUPERCLASS",
    justification = "Compatibility class to be removed in next major release.")
public class HadoopLogCloser extends org.apache.accumulo.server.manager.recovery.HadoopLogCloser {
  private static final Logger log = LoggerFactory.getLogger(HadoopLogCloser.class);

  public HadoopLogCloser() {
    log.warn("{} has been deprecated. Please update property {} to {} instead.",
        getClass().getName(), Property.MANAGER_WAL_CLOSER_IMPLEMENTATION.getKey(),
        org.apache.accumulo.server.manager.recovery.HadoopLogCloser.class.getName());
  }
}
