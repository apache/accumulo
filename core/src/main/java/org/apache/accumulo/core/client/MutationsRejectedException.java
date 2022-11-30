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
package org.apache.accumulo.core.client;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.security.SecurityErrorCode;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.ConstraintViolationSummary;
import org.apache.accumulo.core.data.TabletId;

/**
 * Communicate the failed mutations of a BatchWriter back to the client.
 */
public class MutationsRejectedException extends AccumuloException {
  private static final long serialVersionUID = 1L;

  private final ArrayList<ConstraintViolationSummary> cvsl = new ArrayList<>();
  private final HashMap<TabletId,Set<SecurityErrorCode>> af = new HashMap<>();
  private final HashSet<String> es = new HashSet<>();
  private final int unknownErrors;

  /**
   * Creates Mutations rejected exception
   *
   * @param client AccumuloClient
   * @param cvsList list of constraint violations
   * @param hashMap authorization failures
   * @param serverSideErrors server side errors
   * @param unknownErrors number of unknown errors
   *
   * @since 2.0.0
   */
  public MutationsRejectedException(AccumuloClient client, List<ConstraintViolationSummary> cvsList,
      Map<TabletId,Set<SecurityErrorCode>> hashMap, Collection<String> serverSideErrors,
      int unknownErrors, Throwable cause) {
    super("# constraint violations : " + cvsList.size() + "  security codes: "
        + format(hashMap, (ClientContext) client) + "  # server errors " + serverSideErrors.size()
        + " # exceptions " + unknownErrors, cause);
    this.cvsl.addAll(cvsList);
    this.af.putAll(hashMap);
    this.es.addAll(serverSideErrors);
    this.unknownErrors = unknownErrors;
  }

  private static String format(Map<TabletId,Set<SecurityErrorCode>> hashMap,
      ClientContext context) {
    Map<String,Set<SecurityErrorCode>> result = new HashMap<>();

    for (Entry<TabletId,Set<SecurityErrorCode>> entry : hashMap.entrySet()) {
      TabletId tabletId = entry.getKey();
      String tableInfo = context.getPrintableTableInfoFromId(tabletId.getTable());

      if (!result.containsKey(tableInfo)) {
        result.put(tableInfo, new HashSet<>());
      }

      result.get(tableInfo).addAll(hashMap.get(tabletId));
    }

    return result.toString();
  }

  /**
   * @return the internal list of constraint violations
   */
  public List<ConstraintViolationSummary> getConstraintViolationSummaries() {
    return cvsl;
  }

  /**
   * @return the internal mapping of TabletID to SecurityErrorCodes
   */
  public Map<TabletId,Set<SecurityErrorCode>> getSecurityErrorCodes() {
    return af;
  }

  /**
   *
   * @return A list of servers that had internal errors when mutations were written
   *
   */
  public Collection<String> getErrorServers() {
    return es;
  }

  /**
   *
   * @return a count of unknown exceptions that occurred during processing
   */
  public int getUnknownExceptions() {
    return unknownErrors;
  }
}
