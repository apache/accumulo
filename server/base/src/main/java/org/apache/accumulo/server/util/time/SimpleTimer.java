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

import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

/**
 * Generic singleton timer: don't use it if you are going to do anything that will take very long. Please use it to reduce the number of threads dedicated to
 * simple events.
 *
 */
public class SimpleTimer {

  static class LoggingTimerTask extends TimerTask {

    private Runnable task;

    LoggingTimerTask(Runnable task) {
      this.task = task;
    }

    @Override
    public void run() {
      try {
        task.run();
      } catch (Throwable t) {
        Logger.getLogger(LoggingTimerTask.class).warn("Timer task failed " + task.getClass().getName() + " " + t.getMessage(), t);
      }
    }

  }

  private static SimpleTimer instance;
  private Timer timer;

  public static synchronized SimpleTimer getInstance() {
    if (instance == null)
      instance = new SimpleTimer();
    return instance;
  }

  private SimpleTimer() {
    timer = new Timer("SimpleTimer", true);
  }

  public void schedule(Runnable task, long delay) {
    timer.schedule(new LoggingTimerTask(task), delay);
  }

  public void schedule(Runnable task, long delay, long period) {
    timer.schedule(new LoggingTimerTask(task), delay, period);
  }

}
