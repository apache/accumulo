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
"use strict";

const htmlBanner = '#gcStatusBanner'
const htmlBannerMessage = '#gc-banner-message'
const htmlTable = '#gc-server'
const fileHtmlTable = '#gc-file'
const walHtmlTable = '#gc-wal'

function refresh() {
  refreshServerInformation(getGcView, htmlTable, GC_SERVER_PROCESS_VIEW, htmlBanner,
    htmlBannerMessage);
  refreshServerInformation(getGcFileView, fileHtmlTable, GC_FILE_SERVER_PROCESS_VIEW, htmlBanner,
    htmlBannerMessage);
  refreshServerInformation(getGcWalView, walHtmlTable, GC_WAL_SERVER_PROCESS_VIEW, htmlBanner,
    htmlBannerMessage);
}

$(function () {
  sessionStorage[GC_SERVER_PROCESS_VIEW] = JSON.stringify({
    data: [],
    columns: []
  });

  sessionStorage[GC_FILE_SERVER_PROCESS_VIEW] = JSON.stringify({
    data: [],
    columns: []
  });
  sessionStorage[GC_WAL_SERVER_PROCESS_VIEW] = JSON.stringify({
    data: [],
    columns: []
  });

  refreshServerInformation(getGcView, htmlTable, GC_SERVER_PROCESS_VIEW, htmlBanner,
    htmlBannerMessage);
  refreshServerInformation(getGcFileView, fileHtmlTable, GC_FILE_SERVER_PROCESS_VIEW, htmlBanner,
    htmlBannerMessage);
  refreshServerInformation(getGcWalView, walHtmlTable, GC_WAL_SERVER_PROCESS_VIEW, htmlBanner,
    htmlBannerMessage);
});
