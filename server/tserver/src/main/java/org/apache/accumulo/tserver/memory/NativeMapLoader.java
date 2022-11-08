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
package org.apache.accumulo.tserver.memory;

import java.io.File;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.conf.Property;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class NativeMapLoader {

  private static final Logger log = LoggerFactory.getLogger(NativeMapLoader.class);
  private static final Pattern dotSuffix = Pattern.compile("[.][^.]*$");
  private static final String PROP_NAME = "accumulo.native.lib.path";
  private static final AtomicBoolean loaded = new AtomicBoolean(false);

  // don't allow instantiation
  private NativeMapLoader() {}

  public synchronized static void load() {
    // load at most once; System.exit if loading fails
    if (loaded.compareAndSet(false, true)) {
      if (loadFromSearchPath(System.getProperty(PROP_NAME)) || loadFromSystemLinker()) {
        return;
      }
      log.error(
          "FATAL! Accumulo native libraries were requested but could not"
              + " be be loaded. Either set '{}' to false in accumulo.properties or make"
              + " sure native libraries are created in directories set by the JVM"
              + " system property '{}' in accumulo-env.sh!",
          Property.TSERV_NATIVEMAP_ENABLED, PROP_NAME);
      System.exit(1);
    }
  }

  public static void loadForTest(List<File> locations, Runnable onFail) {
    // if the library can't be loaded at the given path, execute the failure task
    var searchPath = locations.stream().map(File::getAbsolutePath).collect(Collectors.joining(":"));
    if (!loadFromSearchPath(searchPath)) {
      onFail.run();
    }
  }

  /**
   * The specified search path will be used to attempt to load them. Directories will be searched by
   * using the system-specific library naming conventions. A path directly to a file can also be
   * provided. Loading will continue until the search path is exhausted, or until the native
   * libraries are found and successfully loaded, whichever occurs first.
   */
  private static boolean loadFromSearchPath(String searchPath) {
    // Attempt to load from these directories, using standard names, or by an exact file name
    if (searchPath != null) {
      if (Stream.of(searchPath.split(":")).flatMap(NativeMapLoader::mapLibraryNames)
          .anyMatch(NativeMapLoader::loadNativeLib)) {
        return true;
      }
      log.error("Tried and failed to load Accumulo native library from property {} set to {}",
          PROP_NAME, searchPath);
    }
    return false;
  }

  // Check LD_LIBRARY_PATH (DYLD_LIBRARY_PATH on Mac)
  private static boolean loadFromSystemLinker() {
    String propName = "java.library.path";
    String ldLibPath = System.getProperty(propName);
    try {
      System.loadLibrary("accumulo");
      log.info("Loaded native map shared library from property {} set to {}", propName, ldLibPath);
      return true;
    } catch (Exception | UnsatisfiedLinkError e) {
      log.error("Tried and failed to load Accumulo native library from property {} set to {}",
          propName, ldLibPath, e);
      return false;
    }
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "search paths provided by admin")
  private static Stream<File> mapLibraryNames(String name) {
    File base = new File(name);
    if (!base.isDirectory()) {
      return Stream.of(base);
    }
    String libname = System.mapLibraryName("accumulo");
    Stream<String> libs;
    if ("Mac OS X".equals(System.getProperty("os.name"))) {
      // additional supported Mac extensions
      String prefix = dotSuffix.matcher(libname).replaceFirst("");
      libs = Stream.of(libname, prefix + ".dylib", prefix + ".jnilib").distinct();
    }
    libs = Stream.of(libname);
    return libs.map(f -> appendFileToDir(base, f));
  }

  // this is its own method because spotbugs sec-bugs doesn't understand how to suppress lambdas
  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "search paths provided by admin")
  private static File appendFileToDir(File base, String f) {
    return new File(base, f);
  }

  private static boolean loadNativeLib(File libFile) {
    log.debug("Trying to load native map library {}", libFile);
    if (libFile.isFile()) {
      try {
        System.load(libFile.getAbsolutePath());
        log.info("Loaded native map shared library {}", libFile);
        return true;
      } catch (Exception | UnsatisfiedLinkError e) {
        log.error("Tried and failed to load native map library {}", libFile, e);
      }
    } else {
      log.debug("Native map library {} not found or is not a file", libFile);
    }
    return false;
  }

}
