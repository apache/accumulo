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
package org.apache.accumulo.server.metadata;

import java.util.Iterator;

import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.data.ConditionalMutation;

class ConditionalWriterDelegator implements ConditionalWriter {
  private final ConditionalWriter delegate;
  private final ConditionalWriterInterceptor interceptor;

  public ConditionalWriterDelegator(ConditionalWriter delegate,
      ConditionalWriterInterceptor interceptor) {
    this.delegate = delegate;
    this.interceptor = interceptor;
  }

  @Override
  public Iterator<Result> write(Iterator<ConditionalMutation> mutations) {
    mutations = interceptor.beforeWrite(mutations);
    return interceptor.afterWrite(delegate.write(mutations));
  }

  @Override
  public Result write(ConditionalMutation mutation) {
    mutation = interceptor.beforeWrite(mutation);
    return interceptor.afterWrite(delegate.write(mutation));
  }

  @Override
  public void close() {
    interceptor.beforeClose();
    delegate.close();
    interceptor.afterClose();
  }
}
