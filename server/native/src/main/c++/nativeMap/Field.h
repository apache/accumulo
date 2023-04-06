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
#include <stdint.h>
#include <string.h>
#include <jni.h>
#include <string>
#include <string.h>
#include <iostream>
#include <stdlib.h>
#include "BlockAllocator.h"

using namespace std;

#ifndef __FIELD__
#define __FIELD__

struct Field {
  uint8_t *field;
  int32_t len;

  int compare(const uint8_t *d1, int len1, const uint8_t *d2, int len2) const{
    int result = memcmp(d1, d2, len1 < len2 ? len1 : len2);

    if(result != 0)
      return result;
    if(len1 == len2)
      return 0;
    if(len1 < len2)
      return -1;

    return 1;
  }

  Field(){}

  Field(LinkedBlockAllocator *lba, JNIEnv *env, jbyteArray f, int l){
    len = l;
    field=(uint8_t *)lba->allocate(len);
    env->GetByteArrayRegion(f, 0, len, (jbyte *)field);
  }

  Field(LinkedBlockAllocator *lba, JNIEnv *env, jbyteArray f){
    len = env->GetArrayLength(f);
    field=(uint8_t *)lba->allocate(len);
    env->GetByteArrayRegion(f, 0, len, (jbyte *)field);
  }

  Field(uint8_t *f, int32_t l):field(f),len(l){
  }

  Field(const char *cstr){
    //constructor for testing C++
    len = strlen(cstr);
    field=new uint8_t[len];
    memcpy(field, cstr, len);
  }

  Field(LinkedBlockAllocator *lba, const char *cstr){
    //constructor for testing C++
    len = strlen(cstr);
    field=(uint8_t *)lba->allocate(len);
    memcpy(field, cstr, len);
  }

  void set(const char *d, int l){
    if(l < 0 || l > len){
      cerr << "Tried to set field with value that is too long " << l << " " << len << endl;
    }
    memcpy(field, d, l);
    len = l;
  }

  void set(JNIEnv *env, jbyteArray f, int l){
    if(l < 0 || l > len){
      cerr << "Tried to set field with value that is too long " << l << " " << len << endl;
    }
    len = l;
    env->GetByteArrayRegion(f, 0, len, (jbyte *)field);
  }

  int compare(const Field &of) const{
    return compare(field, len, of.field, of.len);
  }

  bool operator<(const Field &of) const{
    return compare(of) < 0;
  }

  int32_t length() const {
    return len;
  }

  void fillIn(JNIEnv *env, jbyteArray d) const {
    //TODO ensure lengths match up
    env->SetByteArrayRegion(d, 0, len, (jbyte *)field);
  }

  jbyteArray createJByteArray(JNIEnv *env) const{
    jbyteArray valData = env->NewByteArray(len);
    env->SetByteArrayRegion(valData, 0, len, (jbyte *)field);
    return valData;
  }

  string toString() const{
    return string((char *)field, len);
  }

  void clear(){
    //delete(field);
  }

  void clear(LinkedBlockAllocator *lba){
    lba->deleteLast(field);
  }
};

struct LocalField : public Field {
  LocalField(JNIEnv *env, jbyteArray f){
    len = env->GetArrayLength(f);
    field= new uint8_t[len];
    env->GetByteArrayRegion(f, 0, len, (jbyte *)field);
  }

  ~LocalField(){
    delete[] field;
  }
};
#endif
