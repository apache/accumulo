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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.accumulo.start.classloader.AccumuloClassLoader;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.accumulo.start.spi.KeywordExecutable.UsageGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  private static final Logger log = LoggerFactory.getLogger(Main.class);
  private static ClassLoader classLoader;
  private static Class<?> vfsClassLoader;
  private static Map<String,KeywordExecutable> servicesMap;

  public static void main(final String[] args) {
    try {
      // Preload classes that cause a deadlock between the ServiceLoader and the DFSClient when using
      // the VFSClassLoader with jars in HDFS.
      ClassLoader loader = getClassLoader();
      Class<?> confClass = null;
      try {
        confClass = AccumuloClassLoader.getClassLoader().loadClass("org.apache.hadoop.conf.Configuration");
      } catch (ClassNotFoundException e) {
        log.error("Unable to find Hadoop Configuration class on classpath, check configuration.", e);
        System.exit(1);
      }
      Object conf = null;
      try {
        conf = confClass.newInstance();
      } catch (Exception e) {
        log.error("Error creating new instance of Hadoop Configuration", e);
        System.exit(1);
      }
      try {
        Method getClassByNameOrNullMethod = conf.getClass().getMethod("getClassByNameOrNull", String.class);
        getClassByNameOrNullMethod.invoke(conf, "org.apache.hadoop.mapred.JobConf");
        getClassByNameOrNullMethod.invoke(conf, "org.apache.hadoop.mapred.JobConfigurable");
      } catch (Exception e) {
        log.error("Error pre-loading JobConf and JobConfigurable classes, VFS classloader with " + "system classes in HDFS may not work correctly", e);
        System.exit(1);
      }

      if (args.length == 0) {
        printUsage();
        System.exit(1);
      }
      if (args[0].equals("-h") || args[0].equals("-help") || args[0].equals("--help")) {
        printUsage();
        System.exit(1);
      }

      // determine whether a keyword was used or a class name, and execute it with the remaining args
      String keywordOrClassName = args[0];
      KeywordExecutable keywordExec = getExecutables(loader).get(keywordOrClassName);
      if (keywordExec != null) {
        execKeyword(keywordExec, stripArgs(args, 1));
      } else {
        execMainClassName(keywordOrClassName, stripArgs(args, 1));
      }

    } catch (Throwable t) {
      log.error("Uncaught exception", t);
      System.exit(1);
    }
  }

  public static synchronized ClassLoader getClassLoader() {
    if (classLoader == null) {
      try {
        ClassLoader clTmp = (ClassLoader) getVFSClassLoader().getMethod("getClassLoader").invoke(null);
        classLoader = clTmp;
        Thread.currentThread().setContextClassLoader(classLoader);
      } catch (ClassNotFoundException | IOException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException
          | SecurityException e) {
        log.error("Problem initializing the class loader", e);
        System.exit(1);
      }
    }
    return classLoader;
  }

  public static synchronized Class<?> getVFSClassLoader() throws IOException, ClassNotFoundException {
    if (vfsClassLoader == null) {
      Thread.currentThread().setContextClassLoader(AccumuloClassLoader.getClassLoader());
      Class<?> vfsClassLoaderTmp = AccumuloClassLoader.getClassLoader().loadClass("org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader");
      vfsClassLoader = vfsClassLoaderTmp;
    }
    return vfsClassLoader;
  }

  private static void execKeyword(final KeywordExecutable keywordExec, final String[] args) {
    Runnable r = () -> {
      try {
        keywordExec.execute(args);
      } catch (Exception e) {
        die(e);
      }
    };
    startThread(r, keywordExec.keyword());
  }

  private static void execMainClassName(final String className, final String[] args) {
    Class<?> classWithMain = null;
    try {
      classWithMain = getClassLoader().loadClass(className);
    } catch (ClassNotFoundException cnfe) {
      System.out.println("Invalid argument: Java <main class> '" + className + "' was not found.  Please use the wholly qualified package name.");
      printUsage();
      System.exit(1);
    }
    execMainClass(classWithMain, args);
  }

  public static void execMainClass(final Class<?> classWithMain, final String[] args) {
    Method main = null;
    try {
      main = classWithMain.getMethod("main", args.getClass());
    } catch (Throwable t) {
      log.error("Could not run main method on '" + classWithMain.getName() + "'.", t);
    }
    if (main == null || !Modifier.isPublic(main.getModifiers()) || !Modifier.isStatic(main.getModifiers())) {
      System.out.println(classWithMain.getName() + " must implement a public static void main(String args[]) method");
      System.exit(1);
    }
    final Method finalMain = main;
    Runnable r = () -> {
      try {
        final Object thisIsJustOneArgument = args;
        finalMain.invoke(null, thisIsJustOneArgument);
      } catch (InvocationTargetException e) {
        if (e.getCause() != null) {
          die(e.getCause());
        } else {
          // Should never happen, but check anyway.
          die(e);
        }
      } catch (Exception e) {
        die(e);
      }
    };
    startThread(r, classWithMain.getName());
  }

  public static String[] stripArgs(final String[] originalArgs, int numToStrip) {
    int newSize = originalArgs.length - numToStrip;
    String newArgs[] = new String[newSize];
    System.arraycopy(originalArgs, numToStrip, newArgs, 0, newSize);
    return newArgs;
  }

  private static void startThread(final Runnable r, final String name) {
    Thread t = new Thread(r, name);
    t.setContextClassLoader(getClassLoader());
    t.start();
  }

  /**
   * Print a stack trace to stderr and exit with a non-zero status.
   *
   * @param t
   *          The {@link Throwable} containing a stack trace to print.
   */
  private static void die(final Throwable t) {
    log.error("Thread '" + Thread.currentThread().getName() + "' died.", t);
    System.exit(1);
  }

  public static void printCommands(TreeSet<KeywordExecutable> set, UsageGroup group) {
    set.stream().filter(e -> e.usageGroup() == group).forEach(ke -> System.out.printf("  %-30s %s\n", ke.usage(), ke.description()));
  }

  public static void printUsage() {
    TreeSet<KeywordExecutable> executables = new TreeSet<>(Comparator.comparing(KeywordExecutable::keyword));
    executables.addAll(getExecutables(getClassLoader()).values());

    System.out.println("\nUsage: accumulo <command> [--help] (<argument> ...)\n\n  --help   Prints usage for specified command");
    System.out.println("\nCore Commands:");
    printCommands(executables, UsageGroup.CORE);

    System.out.println("  <main class> args              Runs Java <main class> located on Accumulo classpath");

    System.out.println("\nProcess Commands:");
    printCommands(executables, UsageGroup.PROCESS);

    System.out.println("\nOther Commands:");
    printCommands(executables, UsageGroup.OTHER);

    System.out.println();
  }

  public static synchronized Map<String,KeywordExecutable> getExecutables(final ClassLoader cl) {
    if (servicesMap == null) {
      servicesMap = checkDuplicates(ServiceLoader.load(KeywordExecutable.class, cl));
    }
    return servicesMap;
  }

  public static Map<String,KeywordExecutable> checkDuplicates(final Iterable<? extends KeywordExecutable> services) {
    TreeSet<String> blacklist = new TreeSet<>();
    TreeMap<String,KeywordExecutable> results = new TreeMap<>();
    for (KeywordExecutable service : services) {
      String keyword = service.keyword();
      if (blacklist.contains(keyword)) {
        // subsequent times a duplicate is found, just warn and exclude it
        warnDuplicate(service);
      } else if (results.containsKey(keyword)) {
        // the first time a duplicate is found, blacklist it and warn
        blacklist.add(keyword);
        warnDuplicate(results.remove(keyword));
        warnDuplicate(service);
      } else {
        // first observance of this keyword, so just add it to the list
        results.put(service.keyword(), service);
      }
    }
    return Collections.unmodifiableSortedMap(results);
  }

  private static void warnDuplicate(final KeywordExecutable service) {
    log.warn("Ambiguous duplicate binding for keyword '{}' found: {}", service.keyword(), service.getClass().getName());
  }
}
