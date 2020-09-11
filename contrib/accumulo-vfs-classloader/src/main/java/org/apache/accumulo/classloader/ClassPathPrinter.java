/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.classloader;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.accumulo.classloader.vfs.ReloadingVFSClassLoader;
import org.apache.accumulo.classloader.vfs.VFSClassLoaderWrapper;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.impl.VFSClassLoader;

public class ClassPathPrinter {

  public interface Printer {
    void print(String s);
  }

  public static void printClassPath(ClassLoader cl, boolean debug) {
    printClassPath(cl, System.out::print, debug);
  }

  public static String getClassPath(ClassLoader cl, boolean debug) {
    StringBuilder cp = new StringBuilder();
    printClassPath(cl, cp::append, debug);
    return cp.toString();
  }

  private static void printJar(Printer out, String jarPath, boolean debug, boolean sawFirst) {
    if (debug) {
      out.print("\t");
    }
    if (!debug && sawFirst) {
      out.print(":");
    }
    out.print(jarPath);
    if (debug) {
      out.print("\n");
    }
  }

  public static void printClassPath(ClassLoader cl, Printer out, boolean debug) {
    try {
      ArrayList<ClassLoader> classloaders = new ArrayList<>();

      while (cl != null) {
        classloaders.add(cl);
        cl = cl.getParent();
      }

      Collections.reverse(classloaders);

      int level = 0;

      for (ClassLoader classLoader : classloaders) {

        level++;

        if (debug && level > 1) {
          out.print("\n");
        }
        if (!debug && level < 2) {
          continue;
        }

        StringBuilder buffer = new StringBuilder();
        buffer.append(level);
        buffer.append(": ");
        buffer.append(classLoader.getName());
        buffer.append(" Classloader ");

        String classLoaderDescription = buffer.toString();
        boolean sawFirst = false;
        if (classLoader.getClass().getName().startsWith("jdk.internal")) {
          if (debug) {
            out.print("Level " + classLoaderDescription + " " + classLoader.getClass().getName()
                + " configuration not inspectable.\n");
          }
        } else if (classLoader instanceof URLClassLoader) {
          if (debug) {
            out.print("Level " + classLoaderDescription + " URL classpath, items are:\n");
          }
          for (URL u : ((URLClassLoader) classLoader).getURLs()) {
            printJar(out, u.getFile(), debug, sawFirst);
            sawFirst = true;
          }
        } else if (classLoader instanceof ReloadingVFSClassLoader) {
          if (debug) {
            out.print("Level " + classLoaderDescription + " VFS classpaths, items are:\n");
          }
          @SuppressWarnings("resource")
          ReloadingVFSClassLoader vcl = (ReloadingVFSClassLoader) classLoader;
          VFSClassLoaderWrapper wrapper = vcl.getWrapper();
          for (FileObject f : wrapper.getFileObjects()) {
            printJar(out, f.getURL().getFile(), debug, sawFirst);
            sawFirst = true;
          }
        } else if (classLoader instanceof VFSClassLoader) {
          if (debug) {
            out.print("Level " + classLoaderDescription + " VFS classpaths, items are:\n");
          }
          VFSClassLoader vcl = (VFSClassLoader) classLoader;
          for (FileObject f : vcl.getFileObjects()) {
            printJar(out, f.getURL().getFile(), debug, sawFirst);
            sawFirst = true;
          }
        } else {
          if (debug) {
            out.print("Unknown classloader configuration " + classLoader.getClass() + "\n");
          }
        }
      }
      out.print("\n");
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

}
