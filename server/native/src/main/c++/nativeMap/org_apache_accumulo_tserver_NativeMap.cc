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
#include "org_apache_accumulo_tserver_NativeMap.h"
#include "SubKey.h"
#include "Field.h"
#include "NativeMap.h"
#include <map>
#include <vector>
#include <jni.h>
#include <iostream>

#ifdef _POSIX_MEMLOCK
#include <sys/mman.h>
#endif

using namespace std;

JNIEXPORT jlong JNICALL Java_org_apache_accumulo_tserver_NativeMap_createNM(JNIEnv *env, jclass cls) {
	return (jlong)(new NativeMap(1<<17, 1<<11));
}

JNIEXPORT jint JNICALL Java_org_apache_accumulo_tserver_NativeMap_sizeNM(JNIEnv *env, jclass cls, jlong nm) {
	return ((NativeMap *)nm)->count;
}

JNIEXPORT jlong JNICALL Java_org_apache_accumulo_tserver_NativeMap_memoryUsedNM(JNIEnv *env, jclass cls, jlong nm) {
	return ((NativeMap *)nm)->getMemoryUsed();
}

JNIEXPORT void JNICALL Java_org_apache_accumulo_tserver_NativeMap_singleUpdate(JNIEnv *env, jclass cls, jlong nm, jbyteArray r, jbyteArray cf, jbyteArray cq, jbyteArray cv, jlong ts, jboolean del, jbyteArray val, jint mutationCount) {
	jlong uid = Java_org_apache_accumulo_tserver_NativeMap_startUpdate(env, cls, nm, r);
	Java_org_apache_accumulo_tserver_NativeMap_update(env, cls, nm, uid, cf, cq, cv, ts, del, val, mutationCount); 
}

JNIEXPORT jlong JNICALL Java_org_apache_accumulo_tserver_NativeMap_startUpdate(JNIEnv *env, jclass cls, jlong nm, jbyteArray r) {
	NativeMap *nativeMap = (NativeMap *)nm;
	ColumnMap *cm = nativeMap->startUpdate(env, r);
	return (jlong)cm;
}

JNIEXPORT void JNICALL Java_org_apache_accumulo_tserver_NativeMap_update(JNIEnv *env, jclass cls, jlong nm, jlong uid, jbyteArray cf, jbyteArray cq, jbyteArray cv, jlong ts, jboolean del, jbyteArray val, jint mutationCount) {
	NativeMap *nativeMap = (NativeMap *)nm;
	nativeMap->update((ColumnMap *)uid, env, cf, cq, cv, ts, del, val, mutationCount);
}

JNIEXPORT jlong JNICALL Java_org_apache_accumulo_tserver_NativeMap_deleteNM(JNIEnv *env, jclass cls, jlong nm) {
	NativeMap *nativeMap = (NativeMap *)nm;
	delete(nativeMap);
  return 0;
}


JNIEXPORT jlong JNICALL Java_org_apache_accumulo_tserver_NativeMap_createNMI__J_3I(JNIEnv *env, jclass cls, jlong nm, jintArray lens) {
	NativeMap *nativeMap = (NativeMap *)nm;
	int32_t ia[7];
	Iterator *iter = nativeMap->iterator(ia);
	if (iter->atEnd()) {
		delete(iter);
		return 0;
	}
	env->SetIntArrayRegion(lens, 0, 7, ia);	
	return (jlong)iter;	
}

JNIEXPORT jlong JNICALL Java_org_apache_accumulo_tserver_NativeMap_createNMI__J_3B_3B_3B_3BJZ_3I(JNIEnv *env, jclass cls, jlong nm, jbyteArray r, jbyteArray cf, jbyteArray cq, jbyteArray cv, jlong ts, jboolean del, jintArray lens) {

  NativeMap *nativeMap = (NativeMap *)nm;
	LocalField row(env, r);
	LocalSubKey sk(env, cf, cq, cv, ts, del);

	int32_t ia[7];
	Iterator *iter = nativeMap->iterator(row, sk, ia);

	if(iter->atEnd()) {
		delete(iter);
		return 0;
	}

	env->SetIntArrayRegion(lens, 0, 7, ia);	
	return (jlong)iter;	
}

JNIEXPORT jboolean JNICALL Java_org_apache_accumulo_tserver_NativeMap_nmiNext(JNIEnv *env, jclass cls, jlong ip, jintArray lens) {
	Iterator &iter = *((Iterator *)ip);

	int32_t ia[7];
	iter.advance(ia);
	if(iter.atEnd()) {
		return false;
	}	

	env->SetIntArrayRegion(lens, 0, 7, ia);	
	return true;
}

JNIEXPORT void JNICALL Java_org_apache_accumulo_tserver_NativeMap_nmiGetData(JNIEnv *env, jclass cls, jlong ip, jbyteArray r, jbyteArray cf, jbyteArray cq, jbyteArray cv, jbyteArray val) {
	Iterator &iter = *((Iterator *)ip);
	if(r != NULL) {
		iter.rowIter->first.fillIn(env, r);
	}

	iter.colIter->first.getCF().fillIn(env, cf);
	iter.colIter->first.getCQ().fillIn(env, cq);
	iter.colIter->first.getCV().fillIn(env, cv);
	iter.colIter->second.fillIn(env, val);
}

JNIEXPORT jlong JNICALL Java_org_apache_accumulo_tserver_NativeMap_nmiGetTS(JNIEnv *env, jclass cls, jlong ip) {
	Iterator &iter = *((Iterator *)ip);
	return iter.colIter->first.getTimestamp();
}

JNIEXPORT void JNICALL Java_org_apache_accumulo_tserver_NativeMap_deleteNMI(JNIEnv *env, jclass cls, jlong ip) {
	delete((Iterator *)ip);	
}

