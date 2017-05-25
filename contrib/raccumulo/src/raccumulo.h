// Copyright 2013 Data Tactics Corporation
//   
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, 
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#ifndef	__raccumulo_h
#define	__raccumulo_h
#define RNOMANGLE extern "C" 
#include <iostream>
#include <unistd.h>
#include <Rversion.h>
#include <R.h>
#include <Rdefines.h>
#include <Rinterface.h>
#include <Rembedded.h>
#include <R_ext/Boolean.h>
#include <R_ext/Parse.h>
#include <R_ext/Rdynload.h>
#include "Accumulo_types.h"

extern "C" {

  SystemPermission::type getSysPerm(const std::string &perm);
  TablePermission::type getTablePerm(const std::string &perm);
  IteratorScope::type getIteratorScope(const std::string &perm);
  PartialKey::type getPartialKey(const std::string &perm);
  inline SEXP getListElement(SEXP list, const char *str);

  boost::shared_ptr<IteratorSetting> getIteratorSetting(SEXP lp);

  void getKey(SEXP lp, Key& retval);
  void getRange(SEXP lp, Range& retval);
  void getScanColumn(SEXP lp, ScanColumn& retval);
  void getScanOptions(SEXP lp, ScanOptions& retval);
  void getBatchScanOptions(SEXP lp, BatchScanOptions& retval);


  SEXP getKeyAsList(Key key);
  SEXP getRangeAsList(Range range);

}

#endif
