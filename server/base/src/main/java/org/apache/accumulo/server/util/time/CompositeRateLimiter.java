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
package org.apache.accumulo.server.util.time;

import java.util.Arrays;
import java.util.List;
import org.apache.accumulo.core.util.RateLimiter;

/** A rate limiter which attends to multiple rate limiters. */
public class CompositeRateLimiter implements RateLimiter {
    private final List<RateLimiter> limiters;
    public CompositeRateLimiter(RateLimiter... limiters) {
        this.limiters=Arrays.asList(limiters);
    }

    @Override
    public long getRate() {
        Long rate=null;
        for (RateLimiter limiter: limiters) {
            long localRate=limiter.getRate();
            if (localRate>0) {
                if (rate==null || localRate<rate) {
                    rate=localRate;
                }
            }
        }
        return rate==null?0:rate;
    }

    @Override
    public void acquire(long permits) {
        for (RateLimiter limiter:limiters) {
            limiter.acquire(permits);
        }
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("Not supported");
    }
}
