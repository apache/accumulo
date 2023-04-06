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
#include <algorithm>
#include <stdint.h>
#include <string.h>
#include <string>
#include <jni.h>

using namespace std;

class Key {

  public:
    int32_t colFamilyOffset;
    int32_t colQualifierOffset;
    int32_t colVisibilityOffset;
    int32_t totalLen;

    uint8_t *keyData;

    int64_t timestamp;
    bool deleted;

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

    /**
     * Constructor for testing purposes
     */

    Key(){}

    Key(const string &r, const string &cf, const string &cq, const string &cv, long ts, bool del){

      colFamilyOffset = r.length();
      colQualifierOffset = colFamilyOffset + cf.length();
      colVisibilityOffset = colQualifierOffset + cq.length();
      totalLen = colVisibilityOffset + cv.length();

      keyData = new uint8_t[totalLen];

      copy(r.begin(), r.end(), keyData);
      copy(cf.begin(), cf.end(), keyData+colFamilyOffset);
      copy(cq.begin(), cq.end(), keyData+colQualifierOffset);
      copy(cv.begin(), cv.end(), keyData+colVisibilityOffset);

      timestamp = ts;
      deleted = del;
    }

    /**
     * Constructor used for taking data from Java Key
     */

    Key(JNIEnv *env, jbyteArray kd, jint cfo, jint cqo, jint cvo, jint tl, jlong ts, jboolean del){

      colFamilyOffset = cfo;
      colQualifierOffset = cqo;
      colVisibilityOffset = cvo;
      totalLen = tl;
      timestamp = ts;
      deleted = del == JNI_TRUE ? true : false;

      keyData = new uint8_t[totalLen];
      env->GetByteArrayRegion(kd, 0, totalLen, (jbyte *)keyData);
    }

    bool operator<(const Key &key) const{
      int result = compare(keyData, colFamilyOffset, key.keyData, key.colFamilyOffset);
      if(result != 0) return result < 0;

      result = compare(keyData + colFamilyOffset, colQualifierOffset - colFamilyOffset, key.keyData + key.colFamilyOffset, key.colQualifierOffset - key.colFamilyOffset);
      if(result != 0) return result < 0;

      result = compare(keyData + colQualifierOffset, colVisibilityOffset - colQualifierOffset, key.keyData + key.colQualifierOffset, key.colVisibilityOffset - key.colQualifierOffset);
      if(result != 0) return result < 0;

      result = compare(keyData + colVisibilityOffset, totalLen - colVisibilityOffset, key.keyData + key.colVisibilityOffset, key.totalLen - key.colVisibilityOffset);
      if(result != 0) return result < 0;

      if(timestamp < key.timestamp){
        return false;
      }else if(timestamp > key.timestamp){
        return true;
      }

      return deleted && !key.deleted;
    }

};


class LocalKey : public Key {

  public:

    JNIEnv *envPtr;
    jbyteArray kd;

    LocalKey(JNIEnv *env, jbyteArray kd, jint cfo, jint cqo, jint cvo, jint tl, jlong ts, jboolean del){
      envPtr = env;

      colFamilyOffset = cfo;
      colQualifierOffset = cqo;
      colVisibilityOffset = cvo;
      totalLen = tl;
      timestamp = ts;
      deleted = del == JNI_TRUE ? true : false;

      this->kd = kd;
      keyData = (uint8_t *)env->GetByteArrayElements((jbyteArray)kd, NULL);
    }

    ~LocalKey(){
      envPtr->ReleaseByteArrayElements(kd, (jbyte *)keyData, JNI_ABORT);
    }

};

