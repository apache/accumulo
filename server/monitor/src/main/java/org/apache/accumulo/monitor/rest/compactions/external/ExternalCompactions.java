/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.monitor.rest.compactions.external;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JSON Object for displaying External Compactions. Variable names become JSON Keys.
 */
public class ExternalCompactions {
  private static Logger log = LoggerFactory.getLogger(ExternalCompactions.class);

  // Variable names become JSON keys
  public boolean ccFound;
  public String ccHost;
  public int numCompactors;
  public List<CompactorInfo> compactors = new ArrayList<>();

  public ExternalCompactions(ExternalCompactionInfo ecInfo) {
    if (ecInfo.getCoordinatorHost().isPresent()) {
      ccFound = true;
      var runningMap = ecInfo.getCompactorsToJobsMap();
      runningMap.forEach((host, job) -> compactors.add(new CompactorInfo(host, job)));
      // var c = ecInfo.getCompactors();
      // c.forEach(h -> compactors.add(new CompactorInfo(h.toString())));
      ccHost = ecInfo.getCoordinatorHost().get().toString();
      numCompactors = compactors.size();
    } else {
      ccFound = false;
    }
  }
}
