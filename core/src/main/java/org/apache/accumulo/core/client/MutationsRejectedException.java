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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.data.ConstraintViolationSummary;
import org.apache.accumulo.core.data.KeyExtent;

/**
 * Communicate the failed mutations of a BatchWriter back to the client.
 * 
 */
public class MutationsRejectedException extends AccumuloException {
  private static final long serialVersionUID = 1L;
   
  private List<ConstraintViolationSummary> cvsl;
  private Map<KeyExtent,Set<SecurityErrorCode>> af;
  private Collection<String> es;
  private int unknownErrors;
  
  /**
   * @param cvsList
   *          list of constraint violations
   * @param hashMap
   *          authorization failures
   * @param serverSideErrors
   *          server side errors
   * @param unknownErrors
   *          number of unknown errors
   */
  public MutationsRejectedException(Instance instance, List<ConstraintViolationSummary> cvsList, HashMap<KeyExtent,Set<SecurityErrorCode>> hashMap,
      Collection<String> serverSideErrors, int unknownErrors, Throwable cause) {
    super("# constraint violations : " + cvsList.size() + "  security codes: " + format(hashMap, instance) + "  # server errors " + serverSideErrors.size()
        + " # exceptions " + unknownErrors, cause);
    this.cvsl = cvsList;
    this.af = hashMap;
    this.es = serverSideErrors;
    this.unknownErrors = unknownErrors;
  }
  
  private static String format(HashMap<KeyExtent,Set<SecurityErrorCode>> hashMap, Instance instance) {
    Map<String, Set<SecurityErrorCode>> errorMap = new HashMap<String,Set<SecurityErrorCode>>();
    
    for(KeyExtent ke : hashMap.keySet()) {
      String tableInfo = Tables.getPrintableTableInfoFromId(instance, ke.getTableId().toString());
      
      if(!errorMap.containsKey(tableInfo)) {
        errorMap.put(tableInfo, new HashSet<SecurityErrorCode>());
      }
      
      errorMap.get(tableInfo).addAll(hashMap.get(ke));
    }
    
    return errorMap.toString();
  }
    
  /**
   * @return the internal list of constraint violations
   */
  public List<ConstraintViolationSummary> getConstraintViolationSummaries() {
    return cvsl;
  }
  
  /**
   * @return the internal list of authorization failures
   * @deprecated since 1.5, see {@link #getAuthorizationFailuresMap()}
   */
  List<KeyExtent> getAuthorizationFailures() {
    return new ArrayList<KeyExtent>(af.keySet());
  }
  
  /**
   * @return the internal mapping of keyextent mappings to SecurityErrorCode
   * @since 1.5.0
   */
  public Map<KeyExtent,Set<SecurityErrorCode>> getAuthorizationFailuresMap() {
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
