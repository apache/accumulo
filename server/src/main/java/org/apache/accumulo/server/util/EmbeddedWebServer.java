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
package org.apache.accumulo.server.util;

import java.lang.reflect.Method;

import javax.servlet.http.HttpServlet;

// Work very hard to make this jetty stuff work with Hadoop 0.20 and 0.19.0 which changed
// the version of jetty from 5.1.4 to 6.1.14.

public class EmbeddedWebServer {
  
  public static EmbeddedWebServer create(int port) throws ClassNotFoundException {
    try {
      return new EmbeddedWebServer5_1(port);
    } catch (ClassNotFoundException ex) {
      return new EmbeddedWebServer6_1(port);
    }
  }
  
  public static EmbeddedWebServer create() throws ClassNotFoundException {
    try {
      return new EmbeddedWebServer5_1();
    } catch (ClassNotFoundException ex) {
      return new EmbeddedWebServer6_1();
    }
  }
  
  public void addServlet(Class<? extends HttpServlet> klass, String where) {}
  
  public int getPort() {
    return 0;
  }
  
  public void start() {}
  
  public void stop() {}
  
  static public class EmbeddedWebServer6_1 extends EmbeddedWebServer {
    // 6.1
    Object server = null;
    Object sock;
    Object handler;
    
    public EmbeddedWebServer6_1() throws ClassNotFoundException {
      this(0);
    }
    
    public EmbeddedWebServer6_1(int port) throws ClassNotFoundException {
      // Works for both
      try {
        ClassLoader loader = this.getClass().getClassLoader();
        server = loader.loadClass("org.mortbay.jetty.Server").getConstructor().newInstance();
        sock = loader.loadClass("org.mortbay.jetty.bio.SocketConnector").getConstructor().newInstance();
        handler = loader.loadClass("org.mortbay.jetty.servlet.ServletHandler").getConstructor().newInstance();
        Method method = sock.getClass().getMethod("setPort", Integer.TYPE);
        method.invoke(sock, port);
      } catch (ClassNotFoundException ex) {
        throw ex;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    
    public void addServlet(Class<? extends HttpServlet> klass, String where) {
      try {
        Method method = handler.getClass().getMethod("addServletWithMapping", klass.getClass(), where.getClass());
        method.invoke(handler, klass, where);
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
    
    public int getPort() {
      try {
        
        Method method = sock.getClass().getMethod("getLocalPort");
        return (Integer) method.invoke(sock);
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
    
    public void start() {
      try {
        Class<?> klass = server.getClass().getClassLoader().loadClass("org.mortbay.jetty.Connector");
        Method method = server.getClass().getMethod("addConnector", klass);
        method.invoke(server, sock);
        klass = server.getClass().getClassLoader().loadClass("org.mortbay.jetty.Handler");
        method = server.getClass().getMethod("setHandler", klass);
        method.invoke(server, handler);
        method = server.getClass().getMethod("start");
        method.invoke(server);
      } catch (Exception e) {
        stop();
        throw new RuntimeException(e);
      }
    }
    
    public void stop() {
      try {
        Method method = server.getClass().getMethod("stop");
        method.invoke(server);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
  
  static public class EmbeddedWebServer5_1 extends EmbeddedWebServer {
    Object sock;
    Object server = null;
    
    public EmbeddedWebServer5_1() throws ClassNotFoundException {
      this(0);
    }
    
    public EmbeddedWebServer5_1(int port) throws ClassNotFoundException {
      Method method;
      try {
        ClassLoader loader = this.getClass().getClassLoader();
        server = loader.loadClass("org.mortbay.jetty.Server").getConstructor().newInstance();
        sock = loader.loadClass("org.mortbay.http.SocketListener").getConstructor().newInstance();
        method = sock.getClass().getMethod("setPort", Integer.TYPE);
        method.invoke(sock, port);
      } catch (ClassNotFoundException ex) {
        throw ex;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      
    }
    
    public void addServlet(Class<? extends HttpServlet> klass, String where) {
      try {
        Method method = server.getClass().getMethod("getContext", String.class);
        Object ctx = method.invoke(server, "/");
        method = ctx.getClass().getMethod("addServlet", String.class, String.class, String.class);
        method.invoke(ctx, where, where, klass.getName());
        Class<?> httpContextClass = this.getClass().getClassLoader().loadClass("org.mortbay.http.HttpContext");
        method = server.getClass().getMethod("addContext", httpContextClass);
        method.invoke(server, ctx);
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
    
    public int getPort() {
      try {
        Method method = sock.getClass().getMethod("getPort");
        return (Integer) method.invoke(sock);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    
    public void start() {
      try {
        Class<?> listener = getClass().getClassLoader().loadClass("org.mortbay.http.HttpListener");
        Method method = server.getClass().getMethod("addListener", listener);
        method.invoke(server, sock);
        method = server.getClass().getMethod("start");
        method.invoke(server);
      } catch (Exception e) {
        stop();
        throw new RuntimeException(e);
      }
    }
    
    public void stop() {
      try {
        Method method = server.getClass().getMethod("stop");
        method.invoke(server);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
