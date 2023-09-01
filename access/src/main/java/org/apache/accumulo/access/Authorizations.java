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
package org.apache.accumulo.access;

import java.util.Collection;
import java.util.Set;

/**
 *
 * @since ????
 */
public class Authorizations {
  private final Set<String> authorizations;

  private Authorizations(Set<String> authorizations) {
    this.authorizations = Set.copyOf(authorizations);
  }

  public Set<String> asSet() {
    return authorizations;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof Authorizations) {
      var oa = (Authorizations) o;
      return authorizations.equals(oa.authorizations);
    }

    return false;
  }

  @Override
  public int hashCode() {
    return authorizations.hashCode();
  }

  public static Authorizations of(String... authorizations) {
    return new Authorizations(Set.of(authorizations));
  }

  public static Authorizations of(Collection<String> authorizations) {
    return new Authorizations(Set.copyOf(authorizations));
  }

}
