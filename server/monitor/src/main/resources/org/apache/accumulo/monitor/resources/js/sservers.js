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
    $, SCAN_SERVER_PROCESS_VIEW, getSserversView, refreshServerInformation
*/
"use strict";

const htmlBanner = '#sserversStatusBanner'
const htmlBannerMessage = '#sservers-banner-message'
const htmlTable = '#sservers'

function refresh() {
  refreshServerInformation(getSserversView, htmlTable, SCAN_SERVER_PROCESS_VIEW, htmlBanner,
    htmlBannerMessage);
}

$(function () {
  sessionStorage[SCAN_SERVER_PROCESS_VIEW] = JSON.stringify({
    data: [],
    columns: [],
    status: null
  });

  refreshServerInformation(getSserversView, htmlTable, SCAN_SERVER_PROCESS_VIEW, htmlBanner,
    htmlBannerMessage);
});
