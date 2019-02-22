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
package org.apache.accumulo.master.tableOps;

import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.fate.Repo;
import org.apache.htrace.TraceScope;

public class TraceRepo<T> implements Repo<T> {

  private static final long serialVersionUID = 1L;

  long traceId;
  long parentId;
  Repo<T> repo;

  public TraceRepo(Repo<T> repo) {
    this.repo = repo;
    TInfo tinfo = TraceUtil.traceInfo();
    traceId = tinfo.traceId;
    parentId = tinfo.parentId;
  }

  @Override
  public long isReady(long tid, T environment) throws Exception {
    try (TraceScope t = TraceUtil.trace(new TInfo(traceId, parentId), repo.getDescription())) {
      return repo.isReady(tid, environment);
    }
  }

  @Override
  public Repo<T> call(long tid, T environment) throws Exception {
    try (TraceScope t = TraceUtil.trace(new TInfo(traceId, parentId), repo.getDescription())) {
      Repo<T> result = repo.call(tid, environment);
      if (result == null)
        return null;
      return new TraceRepo<>(result);
    }
  }

  @Override
  public void undo(long tid, T environment) throws Exception {
    try (TraceScope t = TraceUtil.trace(new TInfo(traceId, parentId), repo.getDescription())) {
      repo.undo(tid, environment);
    }
  }

  @Override
  public String getDescription() {
    return repo.getDescription();
  }

  @Override
  public String getReturn() {
    return repo.getReturn();
  }

}
