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
package org.apache.accumulo.start;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import org.apache.accumulo.start.classloader.AccumuloClassLoader;

public class Main {
  
  public static void main(String[] args) throws Exception {
    Runnable r = null;
    
    try {
      if (args.length == 0) {
        printUsage();
        System.exit(1);
      }
      final String argsToPass[] = new String[args.length - 1];
      System.arraycopy(args, 1, argsToPass, 0, args.length - 1);
      
      Thread.currentThread().setContextClassLoader(AccumuloClassLoader.getClassLoader());
      
      Class<?> vfsClassLoader = AccumuloClassLoader.getClassLoader().loadClass("org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader");
      
      ClassLoader cl = (ClassLoader) vfsClassLoader.getMethod("getClassLoader", new Class[] {}).invoke(null, new Object[] {});
      
      Class<?> runTMP = null;
      
      Thread.currentThread().setContextClassLoader(cl);
      
      if (args[0].equals("master")) {
        runTMP = cl.loadClass("org.apache.accumulo.server.master.Master");
      } else if (args[0].equals("tserver")) {
        runTMP = cl.loadClass("org.apache.accumulo.server.tabletserver.TabletServer");
      } else if (args[0].equals("shell")) {
        runTMP = cl.loadClass("org.apache.accumulo.core.util.shell.Shell");
      } else if (args[0].equals("init")) {
        runTMP = cl.loadClass("org.apache.accumulo.server.util.Initialize");
      } else if (args[0].equals("admin")) {
        runTMP = cl.loadClass("org.apache.accumulo.server.util.Admin");
      } else if (args[0].equals("gc")) {
        runTMP = cl.loadClass("org.apache.accumulo.server.gc.SimpleGarbageCollector");
      } else if (args[0].equals("monitor")) {
        runTMP = cl.loadClass("org.apache.accumulo.server.monitor.Monitor");
      } else if (args[0].equals("tracer")) {
        runTMP = cl.loadClass("org.apache.accumulo.server.trace.TraceServer");
      } else if (args[0].equals("proxy")) {
        runTMP = cl.loadClass("org.apache.accumulo.proxy.Proxy");
      } else if (args[0].equals("classpath")) {
        vfsClassLoader.getMethod("printClassPath", new Class[] {}).invoke(vfsClassLoader, new Object[] {});
        return;
      } else if (args[0].equals("version")) {
        runTMP = cl.loadClass("org.apache.accumulo.core.Constants");
        System.out.println(runTMP.getField("VERSION").get(null));
        return;
      } else if (args[0].equals("rfile-info")) {
        runTMP = cl.loadClass("org.apache.accumulo.core.file.rfile.PrintInfo");
      } else if (args[0].equals("login-info")) {
        runTMP = cl.loadClass("org.apache.accumulo.core.util.LoginProperties");
      } else if (args[0].equals("zookeeper")) {
        runTMP = cl.loadClass("org.apache.accumulo.server.util.ZooKeeperMain");
      } else {
        try {
          runTMP = cl.loadClass(args[0]);
        } catch (ClassNotFoundException cnfe) {
          System.out.println("Classname " + args[0] + " not found.  Please make sure you use the wholly qualified package name.");
          System.exit(1);
        }
      }
      Method main = null;
      try {
        main = runTMP.getMethod("main", args.getClass());
      } catch (Throwable t) {
        t.printStackTrace();
      }
      if (main == null || !Modifier.isPublic(main.getModifiers()) || !Modifier.isStatic(main.getModifiers())) {
        System.out.println(args[0] + " must implement a public static void main(String args[]) method");
        System.exit(1);
      }
      final Object thisIsJustOneArgument = argsToPass;
      final Method finalMain = main;
      r = new Runnable() {
        public void run() {
          try {
            finalMain.invoke(null, thisIsJustOneArgument);
          } catch (Exception e) {
            System.err.println("Thread \"" + Thread.currentThread().getName() + "\" died " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(1);
          }
        }
      };
      
      Thread t = new Thread(r, args[0]);
      t.setContextClassLoader(cl);
      t.start();
    } catch (Throwable t) {
      System.err.println("Uncaught exception: " + t.getMessage());
      t.printStackTrace(System.err);
    }
  }
  
  private static void printUsage() {
    System.out.println("accumulo init | master | tserver | monitor | shell | admin | gc | classpath | rfile-info | login-info | tracer | proxy | zookeeper | <accumulo class> args");
  }
}
