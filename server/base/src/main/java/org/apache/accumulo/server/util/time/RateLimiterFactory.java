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

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableSet;
import java.util.Set;
import java.util.concurrent.Callable;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.util.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RateLimiterFactory implements Runnable {
    private static RateLimiterFactory instance=null;
    private final Logger logger=LoggerFactory.getLogger(RateLimiterFactory.class);
    private final BiMap<Object, GuavaRateLimiter> activeLimiters=HashBiMap.create();

    RateLimiterFactory(SimpleTimer timer) {
        timer.schedule(this, 5000);
    }
    
    public static RateLimiterFactory getInstance(SimpleTimer timer) {
        synchronized (RateLimiterFactory.class) {
            if (instance==null) {
                instance=new RateLimiterFactory(timer);
            }
        }
        return instance;
    }
    
    public static RateLimiterFactory getInstance(AccumuloConfiguration conf) {
        return getInstance(SimpleTimer.getInstance(conf));
    }
    
    /** Lookup the RateLimiter associated with the specified object, or create a new one for that
     * object.  RateLimiters should be closed when no longer needed.
     * @param identity key for the rate limiter
     * @param rateGenerator a function which can be called to get what the current rate for the
     * rate limiter should be.
     */
    public synchronized RateLimiter create(Object identity, Callable<Long> rateGenerator) {
        if (activeLimiters.containsKey(identity)) {
            GuavaRateLimiter limiter=activeLimiters.get(identity);
            limiter.addRef();
            return limiter;
        } else {
            GuavaRateLimiter limiter=new GuavaRateLimiter(rateGenerator);
            activeLimiters.put(identity, limiter);
            return limiter;
        }
    }
    
    @Override
    public void run() {
        Set<GuavaRateLimiter> limiters;
        synchronized (activeLimiters) {
            limiters=ImmutableSet.copyOf(activeLimiters.values());
        }
        for (GuavaRateLimiter limiter:limiters) {
            limiter.update();
        }
    }
    
    protected class GuavaRateLimiter implements RateLimiter {
        private final com.google.common.util.concurrent.RateLimiter rateLimiter;
        private final Callable<Long> rateCallable;
        
        private long referenceCount;
        private long currentRate;
        
        GuavaRateLimiter(Callable<Long> rateCallable) {
            this.referenceCount=1;
            this.rateCallable=rateCallable;
            try {
              this.currentRate=rateCallable.call();
            } catch (Exception ex) {
              throw new IllegalStateException(ex);
            }
            this.rateLimiter=com.google.common.util.concurrent.RateLimiter.create(currentRate==0?1:currentRate);
        }
        
        @Override
        public synchronized long getRate() {
            return currentRate;
        }

        @Override
        public void acquire(long permits) {
            if (currentRate<=0) {
                // Nonpositive rate -> no limit
                return;
            }
            while (permits>Integer.MAX_VALUE) {
                rateLimiter.acquire(Integer.MAX_VALUE);
                permits-=Integer.MAX_VALUE;
            }
            rateLimiter.acquire((int)permits);
        }
        
        /** Increase the reference count for this RateLimiter. */
        protected void addRef() {
            synchronized (RateLimiterFactory.this.activeLimiters) {
                synchronized (this) {
                    referenceCount++;
                }
            }
        }
        
        @Override
        public void close() {
            synchronized (RateLimiterFactory.this.activeLimiters) {
                synchronized (this) {
                    referenceCount--;
                    if (referenceCount==0) {
                        activeLimiters.inverse().remove(this);
                    }
                }
            }
        }
        
        public synchronized void update() {
            try {
                long rate=rateCallable.call();
                if (rate!=currentRate) {
                    currentRate=rate;
                    rateLimiter.setRate(rate);
                }
            } catch (Exception ex) {
                logger.debug("Failed to update rate limiter", ex);
            }
        }
    }
}
