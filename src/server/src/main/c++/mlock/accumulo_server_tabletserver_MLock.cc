#include "accumulo_server_tabletserver_MLock.h"

#include <unistd.h>

#ifdef _POSIX_MEMLOCK
#include <sys/mman.h>
#endif

JNIEXPORT jint JNICALL Java_accumulo_server_tabletserver_MLock_lockMemoryPages(JNIEnv *env, jclass cls){
#if defined(_POSIX_MEMLOCK) && _POSIX_MEMLOCK > 0
     return mlockall(MCL_CURRENT | MCL_FUTURE);
#else
     return -1;
#endif
}
