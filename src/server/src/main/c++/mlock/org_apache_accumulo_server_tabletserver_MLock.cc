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
#include "org_apache_accumulo_server_tabletserver_MLock.h"

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
