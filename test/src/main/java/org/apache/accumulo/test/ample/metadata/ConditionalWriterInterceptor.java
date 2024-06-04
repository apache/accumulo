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
package org.apache.accumulo.test.ample.metadata;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.ConditionalWriter.Result;
import org.apache.accumulo.core.client.ConditionalWriter.Status;
import org.apache.accumulo.core.data.ConditionalMutation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Streams;

public interface ConditionalWriterInterceptor {

  default Iterator<ConditionalMutation> beforeWrite(Iterator<ConditionalMutation> mutations) {
    return mutations;
  }

  default Iterator<Result> afterWrite(Iterator<Result> results) {
    return results;
  }

  default ConditionalMutation beforeWrite(ConditionalMutation mutation) {
    return mutation;
  }

  default Result afterWrite(Result result) {
    return result;
  }

  default void beforeClose() {

  }

  default void afterClose() {

  }

  static ConditionalWriterInterceptor withStatus(Status replaced, int times) {
    return withStatus(null, replaced, times);
  }

  static ConditionalWriterInterceptor withStatus(Status firstExpected, Status replaced, int times) {
    final AtomicInteger count = new AtomicInteger();
    return new ConditionalWriterInterceptor() {
      @Override
      public Iterator<Result> afterWrite(Iterator<Result> results) {
        if (count.getAndIncrement() < times) {
          // For the first run only, make sure each state matches firstExpected if not null
          // for other runs don't check since we are changing state so future runs may not match
          return Streams.stream(results).map(r -> {
            try {
              Preconditions
                  .checkState(times > 1 || firstExpected == null || r.getStatus() == firstExpected);
            } catch (IllegalStateException e) {
              throw e;
            } catch (Exception e) {
              throw new IllegalStateException(e);
            }
            return new Result(replaced, r.getMutation(), r.getTabletServer());
          }).collect(Collectors.toList()).iterator();
        }
        return results;
      }
    };
  }
}
