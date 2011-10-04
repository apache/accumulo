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
    strcpy(trickFile, getenv("HOME"));
    strcat(trickFile, "/");
    strcat(trickFile, "HOLD_IO_");
    sprintf(pid, "%d", getpid());
    strcat(trickFile, pid);
  }

  while (access(trickFile, R_OK) == 0) {
    fprintf(stderr, "sleeping\n");
    fflush(stderr);
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
