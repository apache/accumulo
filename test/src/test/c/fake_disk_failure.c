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
#include <unistd.h>
#include <dlfcn.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

static
void test_pause() {
  static char trickFile[1024] = "";
  static char pid[10] = "";
  if (trickFile[0] == '\0') {
    strcpy(trickFile, getenv("TRICK_FILE"));
  }

  while (access(trickFile, R_OK) == 0) {
    fprintf(stdout, "sleeping\n");
    fflush(stdout);
    sleep(1);
  }
}

ssize_t write(int fd, const void *buf, size_t count) {
  void * real_write = dlsym(RTLD_NEXT, "write");
  ssize_t (*real_write_t)(int, const void*, size_t) = real_write;

  test_pause();
  return real_write_t(fd, buf, count);
}

ssize_t read(int fd, void *buf, size_t count) {
  void * real_read = dlsym(RTLD_NEXT, "read");
  ssize_t (*real_read_t)(int, void*, size_t) = real_read;
  test_pause();
  return real_read_t(fd, buf, count);
}
