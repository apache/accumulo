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
package org.apache.accumulo.master.replication;

import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.Daemon;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.replication.WorkAssigner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Driver for a {@link WorkAssigner}
 */
public class WorkDriver extends Daemon {
  private static final Logger log = LoggerFactory.getLogger(WorkDriver.class);

  private Master master;
  private Connector conn;
  private AccumuloConfiguration conf;

  private WorkAssigner assigner;
  private String assignerImplName;

  public WorkDriver(Master master) throws AccumuloException, AccumuloSecurityException {
    super();
    this.master = master;
    this.conn = master.getConnector();
    this.conf = master.getConfiguration();
    configureWorkAssigner();
  }

  protected void configureWorkAssigner() {
    String workAssignerClass = conf.get(Property.REPLICATION_WORK_ASSIGNER);

    if (null == assigner || !assigner.getClass().getName().equals(workAssignerClass)) {
      log.info("Initializing work assigner implementation of {}", workAssignerClass);

      try {
        Class<?> clz = Class.forName(workAssignerClass);
        Class<? extends WorkAssigner> workAssignerClz = clz.asSubclass(WorkAssigner.class);
        this.assigner = workAssignerClz.newInstance();
      } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
        log.error("Could not instantiate configured work assigner {}", workAssignerClass, e);
        throw new RuntimeException(e);
      }

      this.assigner.configure(conf, conn);
      this.assignerImplName = assigner.getClass().getName();
      this.setName(assigner.getName());
    }
  }

  /*
   * Getters/setters for testing purposes
   */
  protected Connector getConnector() {
    return conn;
  }

  protected void setConnector(Connector conn) {
    this.conn = conn;
  }

  protected AccumuloConfiguration getConf() {
    return conf;
  }

  protected void setConf(AccumuloConfiguration conf) {
    this.conf = conf;
  }

  @Override
  public void run() {
    log.info("Starting replication work assignment thread using {}", assignerImplName);

    while (master.stillMaster()) {
      // Assign the work using the configured implementation
      try {
        assigner.assignWork();
      } catch (Exception e) {
        log.error("Error while assigning work", e);
      }

      long sleepTime = conf.getTimeInMillis(Property.REPLICATION_WORK_ASSIGNMENT_SLEEP);
      log.debug("Sleeping {} ms before next work assignment", sleepTime);
      sleepUninterruptibly(sleepTime, TimeUnit.MILLISECONDS);

      // After each loop, make sure that the WorkAssigner implementation didn't change
      configureWorkAssigner();
    }
  }
}
