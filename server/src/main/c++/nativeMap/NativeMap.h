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
#ifndef __NATIVE_MAP_A12__
#define __NATIVE_MAP_A12__

#include "SubKey.h"
#include "Field.h"
#include "BlockAllocator.h"
#include <map>
#include <vector>
#include <iostream>

using namespace std;

typedef map<SubKey, Field, std::less<SubKey>,  BlockAllocator<std::pair<const SubKey, Field> > > ColumnMap;
typedef map<Field, ColumnMap, std::less<Field>,  BlockAllocator<std::pair<const Field, ColumnMap> > > RowMap;


struct NativeMapData {
	LinkedBlockAllocator *lba;
	RowMap rowmap;
	int count;

	NativeMapData(int blockSize, int bigBlockSize):lba(new LinkedBlockAllocator(blockSize, bigBlockSize)),
							rowmap(RowMap(std::less<Field>(), BlockAllocator<std::pair<Field, ColumnMap> >(lba))){
	}

	~NativeMapData(){
		rowmap.clear(); //if row map is not cleared here, it will be deconstructed after lba is deleted
		delete(lba);
	}
};


struct Iterator {
	NativeMapData &nativeMap;
	RowMap::iterator rowIter;
	ColumnMap::iterator colIter;

	Iterator(NativeMapData &nm, int32_t *ia):nativeMap(nm){
		rowIter = nativeMap.rowmap.begin();
		if(rowIter == nativeMap.rowmap.end()){
			return;
		}

		colIter = rowIter->second.begin();

		skipAndFillIn(ia, true);
	}


	Iterator(NativeMapData &nm, Field &row, SubKey &sk, int32_t *ia):nativeMap(nm){
		rowIter = nativeMap.rowmap.lower_bound(row);
		if(rowIter == nativeMap.rowmap.end()){
			return;
		}

		//TODO use equals instead of compare
		if(rowIter->first.compare(row) == 0){
			colIter = rowIter->second.lower_bound(sk);
		}else{
			colIter = rowIter->second.begin();
		}

		skipAndFillIn(ia, true);
	}

	bool skipAndFillIn(int32_t *ia, bool firstCall)
	{
		bool rowChanged = false;
	
		while(colIter == rowIter->second.end()){
			rowIter++;
			rowChanged = true;
			if(rowIter == nativeMap.rowmap.end()){
				return false;
			}
			colIter = rowIter->second.begin();
		}

		ia[0] = (firstCall || rowChanged) ? rowIter->first.length() : -1;
		ia[1] = colIter->first.getCFLen();
		ia[2] = colIter->first.getCQLen();
		ia[3] = colIter->first.getCVLen();
		ia[4] = colIter->first.isDeleted() ? 1 : 0;
		ia[5] = colIter->second.length();
		ia[6] = colIter->first.getMC();
	
		return true;
	}

	bool atEnd(){
		return rowIter == nativeMap.rowmap.end();
	}

	void advance(int32_t *ia){
		colIter++;
		skipAndFillIn(ia, false);
	}
};

struct NativeMap : public NativeMapData {

	NativeMap(int blockSize, int bigBlockSize):NativeMapData(blockSize, bigBlockSize){
		count = 0;
	}

	~NativeMap(){

	}


	ColumnMap *startUpdate(JNIEnv * env, jbyteArray r){
		Field row(lba, env, r);
		return startUpdate(row);
	}

	ColumnMap *startUpdate(const char *r){

		Field row(lba, r);	
		return startUpdate(row);
	}

	ColumnMap *startUpdate(Field &row){
		//cout << "Starting update "<<row.toString()<<endl;

		pair<RowMap::iterator, bool> insertResult = rowmap.insert(pair<Field, ColumnMap>(row, ColumnMap(std::less<SubKey>(), BlockAllocator<std::pair<SubKey, Field> >(lba))));

		if(!insertResult.second)
			row.clear(lba);

		return &(insertResult.first->second);

	}

	void update(ColumnMap *cm, JNIEnv *env, jbyteArray cf, jbyteArray cq, jbyteArray cv, jlong ts, jboolean del, jbyteArray val, jint mutationCount){

		SubKey sk(lba, env, cf, cq, cv, ts, del, mutationCount);
		//cout << "Updating " << sk.toString() << " " << sk.getTimestamp() << " " << sk.isDeleted() << endl;
		//do not bother allocating value if not needed
		Field value(NULL, 0);

		pair<ColumnMap::iterator, bool> insertResult = cm->insert(pair<SubKey, Field>(sk, value));
		if(insertResult.second){
			insertResult.first->second  = Field(lba, env, val);
			count++;
		}else{
			sk.clear(lba);
			int valLen =  env->GetArrayLength(val);
			if(valLen <= insertResult.first->second.length()){
				insertResult.first->second.set(env, val, valLen);
			}else{
				insertResult.first->second.clear();
				insertResult.first->second  = Field(lba, env, val, valLen);
			} 
		}
	}

	void update(ColumnMap *cm, const char *cf, const char *cq, const char *cv, long ts, bool del, const char *val, int valLen, int mutationCount){

		SubKey sk(lba, cf, cq, cv, ts, del, mutationCount);
		//do not bother allocating value if not needed
		Field value(NULL, 0);

		pair<ColumnMap::iterator, bool> insertResult = cm->insert(pair<SubKey, Field>(sk, value));
		if(insertResult.second){
			insertResult.first->second  = Field(lba, val);
			count++;
		}else{
			sk.clear(lba);
			if(insertResult.first->second.length() <= valLen){
				insertResult.first->second.set(val, valLen);
			}else{
				insertResult.first->second.clear();
				insertResult.first->second  = Field(lba, val);
			} 
		}
	}

	Iterator *iterator(int32_t *ia){
		return new Iterator(*this, ia);
	}

	Iterator *iterator(Field &row, SubKey &sk, int32_t *ia){
		return new Iterator(*this, row, sk, ia);
	}

	int64_t getMemoryUsed(){
	  return lba->getMemoryUsed();
	}
};

#endif

