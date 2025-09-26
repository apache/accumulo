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
package org.apache.accumulo.core.constraints;

import java.util.List;

import org.apache.accumulo.core.data.Mutation;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A constraints that limits the size of keys to 1mb.
 *
 * @deprecated since 2.1.0 Use
 *             {@link org.apache.accumulo.core.data.constraints.DefaultKeySizeConstraint}
 */
@Deprecated(since = "2.1.0")
@SuppressFBWarnings(value = "NM_SAME_SIMPLE_NAME_AS_SUPERCLASS",
    justification = "Same name used for compatibility during deprecation cycle")
public class DefaultKeySizeConstraint extends
    org.apache.accumulo.core.data.constraints.DefaultKeySizeConstraint implements Constraint {

  protected static final short MAX__KEY_SIZE_EXCEEDED_VIOLATION =
      org.apache.accumulo.core.data.constraints.DefaultKeySizeConstraint.MAX__KEY_SIZE_EXCEEDED_VIOLATION;
  protected static final long maxSize =
      org.apache.accumulo.core.data.constraints.DefaultKeySizeConstraint.maxSize;

  @Override
  public List<Short> check(Constraint.Environment env, Mutation mutation) {
    return super.check(env, mutation);
  }
}
