/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.start;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.accumulo.start.spi.KeywordExecutable.UsageGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  private static final Logger log = LoggerFactory.getLogger(Main.class);
  private static ClassLoader classLoader;
  private static Map<UsageGroup,Map<String,KeywordExecutable>> servicesMap;

  public static void main(final String[] args) throws Exception {
    final ClassLoader loader = getClassLoader();

    if (args.length == 0) {
      printUsage();
      System.exit(1);
    }
    if (args[0].equals("-h") || args[0].equals("-help") || args[0].equals("--help")) {
      printUsage();
      return;
    }

    if (args.length == 1) {
      execMainClassName(args[0], new String[] {});
    } else {
      String arg1 = args[0];
      String arg2 = args[1];
      KeywordExecutable keywordExec = null;
      try {
        UsageGroup group = UsageGroup.valueOf(arg1.toUpperCase());
        keywordExec = getExecutables(loader).get(group).get(arg2);
      } catch (IllegalArgumentException e) {}
      if (keywordExec != null) {
        execKeyword(keywordExec, stripArgs(args, 2));
      } else {
        execMainClassName(arg1, stripArgs(args, 1));
      }
    }

  }

  public static synchronized ClassLoader getClassLoader() {
    if (classLoader == null) {
      try {
        classLoader = ClassLoader.getSystemClassLoader();
        Thread.currentThread().setContextClassLoader(classLoader);
      } catch (IllegalArgumentException | SecurityException e) {
        die(e, "Problem initializing the class loader");
      }
    }
    return classLoader;
  }

  private static void execKeyword(final KeywordExecutable keywordExec, final String[] args) {
    Runnable r = () -> {
      try {
        keywordExec.execute(args);
      } catch (Exception e) {
        die(e, null);
      }
    };
    startThread(r, keywordExec.keyword());
  }

  private static void execMainClassName(final String className, final String[] args) {
    Class<?> classWithMain = null;
    try {
      classWithMain = getClassLoader().loadClass(className);
    } catch (ClassNotFoundException cnfe) {
      System.out.println("Invalid argument: Java <main class> '" + className
          + "' was not found.  Please use the wholly qualified package name.");
      printUsage();
      System.exit(1);
    }
    execMainClass(classWithMain, args);
  }

  public static void execMainClass(final Class<?> classWithMain, final String[] args) {
    Method main = null;
    try {
      main = classWithMain.getMethod("main", args.getClass());
    } catch (Exception t) {
      die(t, "Could not run main method on '" + classWithMain.getName() + "'.");
    }
    if (main == null || !Modifier.isPublic(main.getModifiers())
        || !Modifier.isStatic(main.getModifiers())) {
      System.out.println(classWithMain.getName()
          + " must implement a public static void main(String args[]) method");
      System.exit(1);
    }
    final Method finalMain = main;
    Runnable r = () -> {
      try {
        finalMain.invoke(null, (Object) args);
      } catch (InvocationTargetException e) {
        if (e.getCause() != null) {
          die(e.getCause(), null);
        } else {
          // Should never happen, but check anyway.
          die(e, null);
        }
      } catch (Exception e) {
        die(e, null);
      }
    };
    startThread(r, classWithMain.getName());
  }

  public static String[] stripArgs(final String[] originalArgs, int numToStrip) {
    int newSize = originalArgs.length - numToStrip;
    String[] newArgs = new String[newSize];
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
   * @param t The {@link Throwable} containing a stack trace to print.
   */
  private static void die(final Throwable t, String msg) {
    String message =
        (msg == null) ? "Thread '" + Thread.currentThread().getName() + "' died." : msg;
    System.err.println(message);
    t.printStackTrace();
    log.error(message, t);
    System.exit(1);
  }

  public static void printUsage() {

    System.out.println("\nUsage one of:");
    System.out.println("    accumulo --help");
    System.out.println("    accumulo classpath");
    System.out.println("    accumulo jshell (<argument> ...)");
    System.out.println("    accumulo className (<argument> ...)");
    System.out.println("    accumulo <group> <command> [--help] (<argument> ...)\n\n");

    Map<UsageGroup,Map<String,KeywordExecutable>> exectuables = getExecutables(getClassLoader());
    List<UsageGroup> groups = Arrays.asList(UsageGroup.values());
    Collections.sort(groups);
    groups.forEach(g -> {
      System.out.println("\n" + g.name() + " Group Commands:");
      exectuables.get(g).values()
          .forEach(ke -> System.out.printf("  %-30s %s\n", ke.usage(), ke.description()));
    });

    System.out.println();
  }

  public static synchronized Map<UsageGroup,Map<String,KeywordExecutable>>
      getExecutables(final ClassLoader cl) {
    if (servicesMap == null) {
      servicesMap = checkDuplicates(ServiceLoader.load(KeywordExecutable.class, cl));
    }
    return servicesMap;
  }

  private record BanKey(UsageGroup group, String keyword) implements Comparable<BanKey> {
    @Override
    public int compareTo(BanKey o) {
      int result = this.group.compareTo(o.group);
      if (result == 0) {
        result = this.keyword.compareTo(o.keyword);
      }
      return result;
    }
  };

  public static Map<UsageGroup,Map<String,KeywordExecutable>>
      checkDuplicates(final Iterable<? extends KeywordExecutable> services) {
    TreeSet<BanKey> banList = new TreeSet<>();
    EnumMap<UsageGroup,Map<String,KeywordExecutable>> results = new EnumMap<>(UsageGroup.class);
    for (UsageGroup ug : UsageGroup.values()) {
      results.put(ug, new TreeMap<>());
    }
    for (KeywordExecutable service : services) {
      UsageGroup group = service.usageGroup();
      String keyword = service.keyword();
      BanKey bk = new BanKey(group, keyword);
      if (banList.contains(bk)) {
        // subsequent times a duplicate is found, just warn and exclude it
        warnDuplicate(service);
      } else if (results.get(group).containsKey(keyword)) {
        // the first time a duplicate is found, banList it and warn
        banList.add(bk);
        warnDuplicate(results.get(group).remove(keyword));
        warnDuplicate(service);
      } else {
        // first observance of this keyword, so just add it to the list
        results.get(group).put(service.keyword(), service);
      }
    }
    return Collections.unmodifiableMap(results);
  }

  private static void warnDuplicate(final KeywordExecutable service) {
    log.warn("Ambiguous duplicate binding for keyword '{}' found: {}", service.keyword(),
        service.getClass().getName());
  }
}
