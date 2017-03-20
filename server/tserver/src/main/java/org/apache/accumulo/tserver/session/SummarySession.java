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

package org.apache.accumulo.tserver.session;

import java.util.concurrent.Future;

import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.core.summary.SummaryCollection;

public class SummarySession extends Session {

  private Future<SummaryCollection> future;

  public SummarySession(TCredentials credentials, Future<SummaryCollection> future) {
    super(credentials);
    this.future = future;
  }

  public Future<SummaryCollection> getFuture() {
    return future;
  }

  @Override
  public boolean cleanup() {
    return future.cancel(true);
  }
}
