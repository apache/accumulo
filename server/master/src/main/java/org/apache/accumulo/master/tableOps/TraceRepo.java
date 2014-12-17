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

import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.trace.instrument.Span;
import org.apache.accumulo.trace.instrument.Trace;
import org.apache.accumulo.trace.instrument.Tracer;
import org.apache.accumulo.trace.thrift.TInfo;

/**
 * 
 */
public class TraceRepo<T> implements Repo<T> {
  
  private static final long serialVersionUID = 1L;
  
  TInfo tinfo;
  Repo<T> repo;
  
  public TraceRepo(Repo<T> repo) {
    this.repo = repo;
    tinfo = Tracer.traceInfo();
  }
  
  @Override
  public long isReady(long tid, T environment) throws Exception {
    Span span = Trace.trace(tinfo, repo.getDescription());
    try {
      return repo.isReady(tid, environment);
    } finally {
      span.stop();
    }
  }
  
  @Override
  public Repo<T> call(long tid, T environment) throws Exception {
    Span span = Trace.trace(tinfo, repo.getDescription());
    try {
      Repo<T> result = repo.call(tid, environment);
      if (result == null)
        return result;
      return new TraceRepo<T>(result);
    } finally {
      span.stop();
    }
  }
  
  @Override
  public void undo(long tid, T environment) throws Exception {
    Span span = Trace.trace(tinfo, repo.getDescription());
    try {
      repo.undo(tid, environment);
    } finally {
      span.stop();
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
