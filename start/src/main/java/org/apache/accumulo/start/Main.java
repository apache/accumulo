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

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.jar.Attributes;
import java.util.jar.JarFile;

import org.apache.accumulo.start.classloader.AccumuloClassLoader;

public class Main {

  public static void main(String[] args) throws Exception {
    Runnable r = null;

    try {
      if (args.length == 0) {
        printUsage();
        System.exit(1);
      }

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
      } else if (args[0].equals("minicluster")) {
        runTMP = cl.loadClass("org.apache.accumulo.minicluster.MiniAccumuloRunner");
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
        runTMP = cl.loadClass("org.apache.accumulo.server.util.LoginProperties");
      } else if (args[0].equals("zookeeper")) {
        runTMP = cl.loadClass("org.apache.accumulo.server.util.ZooKeeperMain");
      } else if (args[0].equals("create-token")) {
        runTMP = cl.loadClass("org.apache.accumulo.core.util.CreateToken");
      } else if (args[0].equals("info")) {
        runTMP = cl.loadClass("org.apache.accumulo.server.util.Info");
      } else if (args[0].equals("jar")) {
        if (args.length < 2) {
          printUsage();
          System.exit(1);
        }
        try {
          JarFile f = new JarFile(args[1]);
          runTMP = loadClassFromJar(args, f, cl);
        } catch (IOException ioe) {
          System.out.println("File " + args[1] + " could not be found or read.");
          System.exit(1);
        } catch (ClassNotFoundException cnfe) {
          System.out.println("Classname " + (args.length > 2 ? args[2] : "in JAR manifest")
              + " not found.  Please make sure you use the wholly qualified package name.");
          System.exit(1);
        }
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
      int chopArgsCount;
      if (args[0].equals("jar")) {
        if (args.length > 2 && runTMP.getName().equals(args[2])) {
          chopArgsCount = 3;
        } else {
          chopArgsCount = 2;
        }
      } else {
        chopArgsCount = 1;
      }
      String argsToPass[] = new String[args.length - chopArgsCount];
      System.arraycopy(args, chopArgsCount, argsToPass, 0, args.length - chopArgsCount);
      final Object thisIsJustOneArgument = argsToPass;
      final Method finalMain = main;
      r = new Runnable() {
        @Override
        public void run() {
          try {
            finalMain.invoke(null, thisIsJustOneArgument);
          } catch (Exception e) {
            System.err.println("Thread \"" + Thread.currentThread().getName() + "\" died " + e);
            e.printStackTrace(System.err);
            System.exit(1);
          }
        }
      };

      Thread t = new Thread(r, args[0]);
      t.setContextClassLoader(cl);
      t.start();
    } catch (Throwable t) {
      System.err.println("Uncaught exception: " + t);
      t.printStackTrace(System.err);
      System.exit(1);
    }
  }

  private static void printUsage() {
    System.out
        .println("accumulo init | master | tserver | monitor | shell | admin | gc | classpath | rfile-info | login-info | tracer | minicluster | proxy | zookeeper | create-token | info | version | jar <jar> [<main class>] args | <accumulo class> args");
  }

  // feature: will work even if main class isn't in the JAR
  static Class<?> loadClassFromJar(String[] args, JarFile f, ClassLoader cl) throws IOException, ClassNotFoundException {
    ClassNotFoundException explicitNotFound = null;
    if (args.length >= 3) {
      try {
        return cl.loadClass(args[2]); // jar jar-file main-class
      } catch (ClassNotFoundException cnfe) {
        // assume this is the first argument, look for main class in JAR manifest
        explicitNotFound = cnfe;
      }
    }
    String mainClass = f.getManifest().getMainAttributes().getValue(Attributes.Name.MAIN_CLASS);
    if (mainClass == null) {
      if (explicitNotFound != null) {
        throw explicitNotFound;
      }
      throw new ClassNotFoundException("No main class was specified, and the JAR manifest does not specify one");
    }
    return cl.loadClass(mainClass);
  }
}
