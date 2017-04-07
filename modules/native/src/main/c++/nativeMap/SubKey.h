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
#include <jni.h>
#include <limits.h>
#include "Field.h"
#include "BlockAllocator.h"

using namespace std;

#ifndef __SUB_KEY__
#define __SUB_KEY__

class SubKey {

public:

	int32_t colQualifierOffset;
	int32_t colVisibilityOffset;
	int32_t totalLen;
	uint8_t *keyData;	
	int64_t timestamp;
	int32_t mutationCount;
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
	SubKey(LinkedBlockAllocator *lba, const string &cf, const string &cq, const string &cv, int64_t ts, bool del, int32_t mc){

		colQualifierOffset = cf.length();
		colVisibilityOffset = colQualifierOffset + cq.length();
		totalLen = colVisibilityOffset + cv.length();

		keyData = (uint8_t *)lba->allocate(totalLen);

		copy(cf.begin(), cf.end(), keyData);
		copy(cq.begin(), cq.end(), keyData+colQualifierOffset);
		copy(cv.begin(), cv.end(), keyData+colVisibilityOffset);

		timestamp = ts;
		deleted = del;

		mutationCount = mc;
	}

	SubKey(LinkedBlockAllocator *lba, JNIEnv *env, jbyteArray cf, jbyteArray cq, jbyteArray cv, jlong ts, jboolean del, int32_t mc){

		int cfLen = env->GetArrayLength(cf);
		int cqLen = env->GetArrayLength(cq);
		int cvLen = env->GetArrayLength(cv);

		colQualifierOffset = cfLen;
		colVisibilityOffset = colQualifierOffset + cqLen;
		totalLen = colVisibilityOffset + cvLen;

		if(lba == NULL)
			keyData = new uint8_t[totalLen];
		else
			keyData = (uint8_t *)lba->allocate(totalLen);


		env->GetByteArrayRegion(cf, 0, cfLen, (jbyte *)keyData);
		env->GetByteArrayRegion(cq, 0, cqLen, (jbyte *)(keyData+colQualifierOffset));
		env->GetByteArrayRegion(cv, 0, cvLen, (jbyte *)(keyData+colVisibilityOffset));

		timestamp = ts;
		deleted = del;

		mutationCount = mc;
	}


	bool operator<(const SubKey &key) const{

		int result = compare(keyData, colQualifierOffset, key.keyData, key.colQualifierOffset);
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

		if(deleted != key.deleted)
			return deleted && !key.deleted;
	
		return mutationCount > key.mutationCount;
	}

	void clear(){
		//delete(keyData);
	}

	void clear(LinkedBlockAllocator *lba){
		lba->deleteLast(keyData);
	}

	int64_t bytesUsed() const{
		return totalLen + 9;
	}

	bool isDeleted() const{
		return deleted;
	}

	int32_t getCFLen() const{
		return colQualifierOffset;
	}
	
	int32_t getCQLen() const {
		return colVisibilityOffset - colQualifierOffset;
	}

	int32_t getCVLen() const {
		return totalLen - colVisibilityOffset;
	}

	const Field getCF() const{
		return Field(keyData, getCFLen());
	}

	const Field getCQ() const{
		return Field(keyData + colQualifierOffset, getCQLen());
	}

	const Field getCV() const{
		return Field(keyData + colVisibilityOffset, getCVLen());
	}

	string toString() const{
		return getCF().toString()+":"+getCQ().toString()+":"+getCV().toString();
	}

	int64_t getTimestamp() const{
		return timestamp;
	}

	int32_t getMC() const{
		return mutationCount;
	}

};

struct LocalSubKey : public SubKey {
	
	LocalSubKey(JNIEnv *env, jbyteArray cf, jbyteArray cq, jbyteArray cv, jlong ts, jboolean del):SubKey(NULL, env, cf, cq, cv, ts, del, INT_MAX){}

	~LocalSubKey(){
		delete(keyData);
	}
};

#endif
 
