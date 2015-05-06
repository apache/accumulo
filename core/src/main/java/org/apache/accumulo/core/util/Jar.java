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

import java.io.IOException;
import java.util.jar.JarFile;

import org.apache.accumulo.start.Main;
import org.apache.accumulo.start.spi.KeywordExecutable;

import com.google.auto.service.AutoService;

@AutoService(KeywordExecutable.class)
public class Jar implements KeywordExecutable {
  @Override
  public String keyword() {
    return "jar";
  }

  @Override
  public void execute(final String[] args) throws Exception {
    // need at least one argument for the jar file, two arguments if the jar file manifest doesn't specify a main class
    if (args.length == 0) {
      Main.printUsage();
      System.exit(1);
    }
    String jarFileName = args[0];
    String candidateMainClass = args.length > 1 ? args[1] : null;
    Class<?> mainClass = null;
    try {
      JarFile f = new JarFile(jarFileName);
      mainClass = Main.loadClassFromJar(args, f, Main.getClassLoader());
    } catch (IOException ioe) {
      System.out.println("File " + jarFileName + " could not be found or read.");
      System.exit(1);
    } catch (ClassNotFoundException cnfe) {
      System.out.println("Classname " + (candidateMainClass != null ? candidateMainClass : "in JAR manifest")
          + " not found.  Please make sure you use the wholly qualified package name.");
      System.exit(1);
    }
    // strip the jar file name and, if specified, the main class name from the args; then execute
    String[] newArgs = Main.stripArgs(args, mainClass.getName().equals(candidateMainClass) ? 2 : 1);
    Main.execMainClass(mainClass, newArgs);
  }
}
