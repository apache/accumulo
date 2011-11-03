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
/**
 * 
 */
package org.apache.accumulo.server.tabletserver;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.master.thrift.TimeType;
import org.apache.accumulo.server.util.time.RelativeTime;

public abstract class TabletTime {
  public static final char LOGICAL_TIME_ID = 'L';
  public static final char MILLIS_TIME_ID = 'M';
  
  public static char getTimeID(TimeType timeType) {
    switch (timeType) {
      case LOGICAL:
        return LOGICAL_TIME_ID;
      case MILLIS:
        return MILLIS_TIME_ID;
    }
    
    throw new IllegalArgumentException("Unknown time type " + timeType);
  }
  
  abstract void useMaxTimeFromWALog(long time);
  
  abstract String getMetadataValue(long time);
  
  abstract String getMetadataValue();
  
  // abstract long setUpdateTimes(Mutation mutation);
  abstract long setUpdateTimes(List<Mutation> mutations);
  
  abstract long getTime();
  
  protected void setSystemTimes(Mutation mutation, long lastCommitTime) {
    Collection<ColumnUpdate> updates = mutation.getUpdates();
    for (ColumnUpdate cvp : updates) {
      if (!cvp.hasTimestamp()) {
        cvp.setSystemTimestamp(lastCommitTime);
        
      }
    }
  }
  
  static TabletTime getInstance(String metadataValue) {
    if (metadataValue.charAt(0) == LOGICAL_TIME_ID) {
      return new LogicalTime(Long.parseLong(metadataValue.substring(1)));
    } else if (metadataValue.charAt(0) == MILLIS_TIME_ID) {
      return new MillisTime(Long.parseLong(metadataValue.substring(1)));
    }
    
    throw new IllegalArgumentException("Time type unknown : " + metadataValue);
    
  }
  
  static class MillisTime extends TabletTime {
    
    private long lastTime;
    private long lastUpdateTime = 0;
    
    public MillisTime(long time) {
      this.lastTime = time;
    }
    
    @Override
    String getMetadataValue(long time) {
      return MILLIS_TIME_ID + "" + time;
    }
    
    @Override
    public String getMetadataValue() {
      return getMetadataValue(lastTime);
    }
    
    @Override
    void useMaxTimeFromWALog(long time) {
      if (time < lastTime)
        throw new IllegalStateException("Existing time " + this.lastTime + " > " + time);
      
      lastTime = time;
    }
    
    @Override
    long setUpdateTimes(List<Mutation> mutations) {
      
      long currTime = RelativeTime.currentTimeMillis();
      
      synchronized (this) {
        if (mutations.size() == 0)
          return lastTime;
        
        if (currTime < lastTime) {
          if (currTime - lastUpdateTime > 0) {
            // not in same millisecond as last call
            // to this method so move ahead slowly
            lastTime++;
          }
          
          lastUpdateTime = currTime;
          
          currTime = lastTime;
        } else {
          lastTime = currTime;
        }
      }
      
      for (Mutation mutation : mutations)
        setSystemTimes(mutation, currTime);
      
      return currTime;
    }
    
    @Override
    long getTime() {
      return lastTime;
    }
    
  }
  
  static class LogicalTime extends TabletTime {
    AtomicLong nextTime;
    
    private LogicalTime(Long time) {
      this.nextTime = new AtomicLong(time.longValue() + 1);
    }
    
    @Override
    void useMaxTimeFromWALog(long time) {
      time++;
      
      if (this.nextTime.get() < time) {
        this.nextTime.set(time);
      } else {
        throw new IllegalStateException("Existing time " + this.nextTime + " >= " + time);
      }
    }
    
    @Override
    public String getMetadataValue() {
      return getMetadataValue(getTime());
    }
    
    @Override
    public String getMetadataValue(long time) {
      return LOGICAL_TIME_ID + "" + time;
    }
    
    @Override
    long setUpdateTimes(List<Mutation> mutations) {
      if (mutations.size() == 0)
        return getTime();
      
      long time = nextTime.getAndAdd(mutations.size());
      for (Mutation mutation : mutations)
        setSystemTimes(mutation, time++);
      
      return time - 1;
    }
    
    @Override
    long getTime() {
      return nextTime.get() - 1;
    }
  }
  
}