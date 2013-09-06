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
package org.apache.accumulo.server.trace;

import java.io.IOException;
import java.net.URI;

import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.accumulo.trace.instrument.Span;
import org.apache.accumulo.trace.instrument.Trace;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

// If FileSystem was an interface, we could use a Proxy, but it's not, so we have to override everything manually

public class TraceFileSystem extends FileSystem {

  @Override
  public void setConf(Configuration conf) {
    Span span = Trace.start("setConf");
    try {
      if (impl != null)
        impl.setConf(conf);
      else
        super.setConf(conf);
    } finally {
      span.stop();
    }
  }

  @Override
  public Configuration getConf() {
    Span span = Trace.start("getConf");
    try {
      return impl.getConf();
    } finally {
      span.stop();
    }
  }

  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {
    Span span = Trace.start("getFileBlockLocations");
    try {
      return impl.getFileBlockLocations(file, start, len);
    } finally {
      span.stop();
    }
  }

  @Override
  public FSDataInputStream open(Path f) throws IOException {
    Span span = Trace.start("open");
    if (Trace.isTracing())
      span.data("path", f.toString());
    try {
      return new TraceFSDataInputStream(impl.open(f));
    } finally {
      span.stop();
    }
  }

  @Override
  public FSDataOutputStream create(Path f) throws IOException {
    Span span = Trace.start("create");
    if (Trace.isTracing())
      span.data("path", f.toString());
    try {
      return impl.create(f);
    } finally {
      span.stop();
    }
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite) throws IOException {
    Span span = Trace.start("create");
    if (Trace.isTracing())
      span.data("path", f.toString());
    try {
      return impl.create(f, overwrite);
    } finally {
      span.stop();
    }
  }

  @Override
  public FSDataOutputStream create(Path f, Progressable progress) throws IOException {
    Span span = Trace.start("create");
    if (Trace.isTracing())
      span.data("path", f.toString());
    try {

      return impl.create(f, progress);
    } finally {
      span.stop();
    }
  }

  @Override
  public FSDataOutputStream create(Path f, short replication) throws IOException {
    Span span = Trace.start("create");
    if (Trace.isTracing())
      span.data("path", f.toString());
    try {
      return impl.create(f, replication);
    } finally {
      span.stop();
    }
  }

  @Override
  public FSDataOutputStream create(Path f, short replication, Progressable progress) throws IOException {
    Span span = Trace.start("create");
    if (Trace.isTracing())
      span.data("path", f.toString());
    try {
      return impl.create(f, replication, progress);
    } finally {
      span.stop();
    }
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize) throws IOException {
    Span span = Trace.start("create");
    if (Trace.isTracing())
      span.data("path", f.toString());
    try {
      return impl.create(f, overwrite, bufferSize);
    } finally {
      span.stop();
    }
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, Progressable progress) throws IOException {
    Span span = Trace.start("create");
    if (Trace.isTracing())
      span.data("path", f.toString());
    try {
      return impl.create(f, overwrite, bufferSize, progress);
    } finally {
      span.stop();
    }
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize) throws IOException {
    Span span = Trace.start("create");
    if (Trace.isTracing())
      span.data("path", f.toString());
    try {
      return impl.create(f, overwrite, bufferSize, replication, blockSize);
    } finally {
      span.stop();
    }
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    Span span = Trace.start("create");
    if (Trace.isTracing())
      span.data("path", f.toString());
    try {
      return impl.create(f, overwrite, bufferSize, replication, blockSize, progress);
    } finally {
      span.stop();
    }
  }

  @Override
  public boolean createNewFile(Path f) throws IOException {
    Span span = Trace.start("createNewFile");
    if (Trace.isTracing())
      span.data("path", f.toString());
    try {
      return impl.createNewFile(f);
    } finally {
      span.stop();
    }
  }

  @Override
  public FSDataOutputStream append(Path f) throws IOException {
    Span span = Trace.start("append");
    if (Trace.isTracing())
      span.data("path", f.toString());
    try {
      return impl.append(f);
    } finally {
      span.stop();
    }
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize) throws IOException {
    Span span = Trace.start("append");
    if (Trace.isTracing())
      span.data("path", f.toString());
    try {
      return impl.append(f, bufferSize);
    } finally {
      span.stop();
    }
  }

  @Deprecated
  @Override
  public short getReplication(Path src) throws IOException {
    Span span = Trace.start("getReplication");
    if (Trace.isTracing())
      span.data("path", src.toString());
    try {
      return impl.getFileStatus(src).getReplication();
    } finally {
      span.stop();
    }
  }

  @Override
  public boolean setReplication(Path src, short replication) throws IOException {
    Span span = Trace.start("setReplication");
    if (Trace.isTracing())
      span.data("path", src.toString());
    try {
      return impl.setReplication(src, replication);
    } finally {
      span.stop();
    }
  }

  @Override
  public boolean exists(Path f) throws IOException {
    Span span = Trace.start("exists");
    if (Trace.isTracing())
      span.data("path", f.toString());
    try {
      return impl.exists(f);
    } finally {
      span.stop();
    }
  }

  @Deprecated
  @Override
  public boolean isDirectory(Path f) throws IOException {
    Span span = Trace.start("isDirectory");
    if (Trace.isTracing())
      span.data("path", f.toString());
    try {
      return impl.getFileStatus(f).isDir();
    } finally {
      span.stop();
    }
  }

  @Override
  public boolean isFile(Path f) throws IOException {
    Span span = Trace.start("isFile");
    if (Trace.isTracing())
      span.data("path", f.toString());
    try {
      return impl.isFile(f);
    } finally {
      span.stop();
    }
  }

  @SuppressWarnings("deprecation")
  @Override
  public long getLength(Path f) throws IOException {
    Span span = Trace.start("getLength");
    if (Trace.isTracing())
      span.data("path", f.toString());
    try {
      return impl.getLength(f);
    } finally {
      span.stop();
    }
  }

  @Override
  public ContentSummary getContentSummary(Path f) throws IOException {
    Span span = Trace.start("getContentSummary");
    if (Trace.isTracing())
      span.data("path", f.toString());
    try {
      return impl.getContentSummary(f);
    } finally {
      span.stop();
    }
  }

  @Override
  public FileStatus[] listStatus(Path f, PathFilter filter) throws IOException {
    Span span = Trace.start("listStatus");
    if (Trace.isTracing())
      span.data("path", f.toString());
    try {
      return impl.listStatus(f, filter);
    } finally {
      span.stop();
    }
  }

  @Override
  public FileStatus[] listStatus(Path[] files) throws IOException {
    Span span = Trace.start("listStatus");
    try {
      return impl.listStatus(files);
    } finally {
      span.stop();
    }
  }

  @Override
  public FileStatus[] listStatus(Path[] files, PathFilter filter) throws IOException {
    Span span = Trace.start("listStatus");
    try {
      return impl.listStatus(files, filter);
    } finally {
      span.stop();
    }
  }

  @Override
  public FileStatus[] globStatus(Path pathPattern) throws IOException {
    Span span = Trace.start("globStatus");
    if (Trace.isTracing())
      span.data("pattern", pathPattern.toString());
    try {
      return impl.globStatus(pathPattern);
    } finally {
      span.stop();
    }
  }

  @Override
  public FileStatus[] globStatus(Path pathPattern, PathFilter filter) throws IOException {
    Span span = Trace.start("globStatus");
    if (Trace.isTracing())
      span.data("pattern", pathPattern.toString());
    try {
      return impl.globStatus(pathPattern, filter);
    } finally {
      span.stop();
    }
  }

  @Override
  public Path getHomeDirectory() {
    Span span = Trace.start("getHomeDirectory");
    try {
      return impl.getHomeDirectory();
    } finally {
      span.stop();
    }
  }

  @Override
  public boolean mkdirs(Path f) throws IOException {
    Span span = Trace.start("mkdirs");
    if (Trace.isTracing())
      span.data("path", f.toString());
    try {
      return impl.mkdirs(f);
    } finally {
      span.stop();
    }
  }

  @Override
  public void copyFromLocalFile(Path src, Path dst) throws IOException {
    Span span = Trace.start("copyFromLocalFile");
    if (Trace.isTracing()) {
      span.data("src", src.toString());
      span.data("dst", dst.toString());
    }
    try {
      impl.copyFromLocalFile(src, dst);
    } finally {
      span.stop();
    }
  }

  @Override
  public void moveFromLocalFile(Path[] srcs, Path dst) throws IOException {
    Span span = Trace.start("moveFromLocalFile");
    if (Trace.isTracing()) {
      span.data("dst", dst.toString());
    }
    try {
      impl.moveFromLocalFile(srcs, dst);
    } finally {
      span.stop();
    }
  }

  @Override
  public void moveFromLocalFile(Path src, Path dst) throws IOException {
    Span span = Trace.start("moveFromLocalFile");
    if (Trace.isTracing()) {
      span.data("src", src.toString());
      span.data("dst", dst.toString());
    }
    try {
      impl.moveFromLocalFile(src, dst);
    } finally {
      span.stop();
    }
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
    Span span = Trace.start("copyFromLocalFile");
    if (Trace.isTracing()) {
      span.data("src", src.toString());
      span.data("dst", dst.toString());
    }
    try {
      impl.copyFromLocalFile(delSrc, src, dst);
    } finally {
      span.stop();
    }
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path[] srcs, Path dst) throws IOException {
    Span span = Trace.start("copyFromLocalFile");
    if (Trace.isTracing()) {
      span.data("dst", dst.toString());
    }
    try {
      impl.copyFromLocalFile(delSrc, overwrite, srcs, dst);
    } finally {
      span.stop();
    }
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src, Path dst) throws IOException {
    Span span = Trace.start("copyFromLocalFile");
    if (Trace.isTracing()) {
      span.data("src", src.toString());
      span.data("dst", dst.toString());
    }
    try {
      impl.copyFromLocalFile(delSrc, overwrite, src, dst);
    } finally {
      span.stop();
    }
  }

  @Override
  public void copyToLocalFile(Path src, Path dst) throws IOException {
    Span span = Trace.start("copyFromLocalFile");
    if (Trace.isTracing()) {
      span.data("src", src.toString());
      span.data("dst", dst.toString());
    }
    try {
      impl.copyToLocalFile(src, dst);
    } finally {
      span.stop();
    }
  }

  @Override
  public void moveToLocalFile(Path src, Path dst) throws IOException {
    Span span = Trace.start("moveToLocalFile");
    if (Trace.isTracing()) {
      span.data("src", src.toString());
      span.data("dst", dst.toString());
    }
    try {
      impl.moveToLocalFile(src, dst);
    } finally {
      span.stop();
    }
  }

  @Override
  public void copyToLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
    Span span = Trace.start("copyToLocalFile");
    if (Trace.isTracing()) {
      span.data("src", src.toString());
      span.data("dst", dst.toString());
    }
    try {
      impl.copyToLocalFile(delSrc, src, dst);
    } finally {
      span.stop();
    }
  }

  @Override
  public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile) throws IOException {
    Span span = Trace.start("startLocalOutput");
    if (Trace.isTracing()) {
      span.data("out", fsOutputFile.toString());
      span.data("local", tmpLocalFile.toString());
    }
    try {
      return impl.startLocalOutput(fsOutputFile, tmpLocalFile);
    } finally {
      span.stop();
    }
  }

  @Override
  public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile) throws IOException {
    Span span = Trace.start("completeLocalOutput");
    if (Trace.isTracing()) {
      span.data("out", fsOutputFile.toString());
      span.data("local", tmpLocalFile.toString());
    }
    try {
      impl.completeLocalOutput(fsOutputFile, tmpLocalFile);
    } finally {
      span.stop();
    }
  }

  @Override
  public void close() throws IOException {
    Span span = Trace.start("close");
    try {
      impl.close();
    } finally {
      span.stop();
    }
  }

  @Override
  public long getUsed() throws IOException {
    Span span = Trace.start("getUsed");
    try {
      return impl.getUsed();
    } finally {
      span.stop();
    }
  }

  @SuppressWarnings("deprecation")
  @Override
  public long getBlockSize(Path f) throws IOException {
    Span span = Trace.start("getBlockSize");
    if (Trace.isTracing()) {
      span.data("path", f.toString());
    }
    try {
      return impl.getBlockSize(f);
    } finally {
      span.stop();
    }
  }

  @Deprecated
  @Override
  public long getDefaultBlockSize() {
    Span span = Trace.start("getDefaultBlockSize");
    try {
      return impl.getDefaultBlockSize();
    } finally {
      span.stop();
    }
  }

  @Deprecated
  @Override
  public short getDefaultReplication() {
    Span span = Trace.start("getDefaultReplication");
    try {
      return impl.getDefaultReplication();
    } finally {
      span.stop();
    }
  }

  @Override
  public FileChecksum getFileChecksum(Path f) throws IOException {
    Span span = Trace.start("getFileChecksum");
    if (Trace.isTracing()) {
      span.data("path", f.toString());
    }
    try {
      return impl.getFileChecksum(f);
    } finally {
      span.stop();
    }
  }

  @Override
  public void setVerifyChecksum(boolean verifyChecksum) {
    Span span = Trace.start("setVerifyChecksum");
    try {
      impl.setVerifyChecksum(verifyChecksum);
    } finally {
      span.stop();
    }
  }

  @Override
  public void setPermission(Path p, FsPermission permission) throws IOException {
    Span span = Trace.start("setPermission");
    if (Trace.isTracing()) {
      span.data("path", p.toString());
    }
    try {
      impl.setPermission(p, permission);
    } finally {
      span.stop();
    }
  }

  @Override
  public void setOwner(Path p, String username, String groupname) throws IOException {
    Span span = Trace.start("setOwner");
    if (Trace.isTracing()) {
      span.data("path", p.toString());
      span.data("user", username);
      span.data("group", groupname);
    }

    try {
      impl.setOwner(p, username, groupname);
    } finally {
      span.stop();
    }
  }

  @Override
  public void setTimes(Path p, long mtime, long atime) throws IOException {
    Span span = Trace.start("setTimes");
    try {
      impl.setTimes(p, mtime, atime);
    } finally {
      span.stop();
    }
  }

  final FileSystem impl;

  TraceFileSystem(FileSystem impl) {
    ArgumentChecker.notNull(impl);
    this.impl = impl;
  }

  public FileSystem getImplementation() {
    return impl;
  }

  @Override
  public URI getUri() {
    Span span = Trace.start("getUri");
    try {
      return impl.getUri();
    } finally {
      span.stop();
    }
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    Span span = Trace.start("open");
    try {
      return new TraceFSDataInputStream(impl.open(f, bufferSize));
    } finally {
      span.stop();
    }
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)
      throws IOException {
    Span span = Trace.start("create");
    try {
      return impl.create(f, overwrite, bufferSize, replication, blockSize, progress);
    } finally {
      span.stop();
    }
  }

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    Span span = Trace.start("initialize");
    try {
      impl.initialize(name, conf);
    } finally {
      span.stop();
    }
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
    Span span = Trace.start("append");
    try {
      return impl.append(f, bufferSize, progress);
    } finally {
      span.stop();
    }
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    Span span = Trace.start("rename");
    try {
      return impl.rename(src, dst);
    } finally {
      span.stop();
    }
  }

  @SuppressWarnings("deprecation")
  @Override
  public boolean delete(Path f) throws IOException {
    Span span = Trace.start("delete");
    try {
      return impl.delete(f);
    } finally {
      span.stop();
    }
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    Span span = Trace.start("delete");
    try {
      return impl.delete(f, recursive);
    } finally {
      span.stop();
    }
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    Span span = Trace.start("listStatus");
    try {
      return impl.listStatus(f);
    } finally {
      span.stop();
    }
  }

  @Override
  public void setWorkingDirectory(Path new_dir) {
    Span span = Trace.start("setWorkingDirectory");
    try {
      impl.setWorkingDirectory(new_dir);
    } finally {
      span.stop();
    }
  }

  @Override
  public Path getWorkingDirectory() {
    Span span = Trace.start("getWorkingDirectory");
    try {
      return impl.getWorkingDirectory();
    } finally {
      span.stop();
    }
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    Span span = Trace.start("mkdirs");
    try {
      return impl.mkdirs(f, permission);
    } finally {
      span.stop();
    }
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    Span span = Trace.start("getFileStatus");
    try {
      return impl.getFileStatus(f);
    } finally {
      span.stop();
    }
  }

  public static FileSystem wrap(FileSystem fileSystem) {
    return new TraceFileSystem(fileSystem);
  }

  public static FileSystem getAndWrap(Configuration conf) throws IOException {
    return wrap(FileSystem.get(conf));
  }

}
