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
package org.apache.accumulo.core.util;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.AsyncAppender;
import org.apache.log4j.net.SocketAppender;
import org.apache.log4j.spi.LoggingEvent;

/**
 * An asynchronous appender that maintains its own internal socket appender. Unlike <code>AsyncAppender</code>, this appender can be configured with a Log4J
 * properties file, although in that case no additional appenders can be added.
 */
public class AsyncSocketAppender extends AsyncAppender {

  private final SocketAppender socketAppender;
  private final AtomicBoolean activated = new AtomicBoolean(false);

  /**
   * Creates a new appender.
   */
  public AsyncSocketAppender() {
    socketAppender = new SocketAppender();
  }

  /**
   * Creates a new appender using the given socket appender internally. Use this constructor for testing only.
   */
  AsyncSocketAppender(SocketAppender socketAppender) {
    this.socketAppender = socketAppender;
  }

  @Override
  public void append(final LoggingEvent event) {
    // Lazy attachment, to avoid calling non-final method in constructor
    if (!isAttached(socketAppender)) {
      addAppender(socketAppender);
    }

    // Lazy activation / connection too, to allow setting host and port
    if (activated.compareAndSet(false, true)) {
      socketAppender.activateOptions();
    }

    super.append(event);
  }

  // SocketAppender delegate methods

  public String getApplication() {
    return socketAppender.getApplication();
  }

  // super.getLocationInfo() will always agree with socketAppender
  public int getPort() {
    return socketAppender.getPort();
  }

  public int getReconnectionDelay() {
    return socketAppender.getReconnectionDelay();
  }

  public String getRemoteHost() {
    return socketAppender.getRemoteHost();
  }

  public boolean isAdvertiseViaMulticastDNS() {
    return socketAppender.isAdvertiseViaMulticastDNS();
  }

  public void setAdvertiseViaMulticastDNS(boolean advertiseViaMulticastDNS) {
    socketAppender.setAdvertiseViaMulticastDNS(advertiseViaMulticastDNS);
  }

  public void setApplication(String lapp) {
    socketAppender.setApplication(lapp);
  }

  @Override
  public void setLocationInfo(boolean locationInfo) {
    super.setLocationInfo(locationInfo);
    socketAppender.setLocationInfo(locationInfo);
  }

  public void setPort(int port) {
    socketAppender.setPort(port);
  }

  public void setReconnectionDelay(int delay) {
    socketAppender.setReconnectionDelay(delay);
  }

  public void setRemoteHost(String host) {
    socketAppender.setRemoteHost(host);
  }
}
