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
/* JSLint global definitions */
/*global
    $, COMPACTOR_SERVER_PROCESS_VIEW, getCompactorsView, refreshServerInformation
*/
"use strict";

const htmlBanner = '#compactorsStatusBanner'
const htmlBannerMessage = '#compactors-banner-message'
const htmlTable = '#compactorsTable'
const visibleColumnFilter = (col) => col != "Server Type";

function refresh() {
  refreshServerInformation(getCompactorsView, htmlTable, COMPACTOR_SERVER_PROCESS_VIEW, htmlBanner, htmlBannerMessage, visibleColumnFilter);
}

$(function () {
  sessionStorage[SCAN_SERVER_PROCESS_VIEW] = JSON.stringify({
    data: [],
    columns: [],
    status: null
  });

  refreshServerInformation(getCompactorsView, htmlTable, COMPACTOR_SERVER_PROCESS_VIEW, htmlBanner, htmlBannerMessage, visibleColumnFilter);
});
