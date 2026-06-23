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

const htmlServerMetricsTable = '#serverMetrics';
const htmlServerMetricsBanner = '#serverMetricsStatusBanner';
const htmlServerMetricsBannerMessage = '#server-metrics-banner-message';

function refreshServerMetrics() {
  if (!serverMetricsType || !serverMetricsResourceGroup || !serverMetricsAddress) {
    sessionStorage[SERVER_METRICS] = JSON.stringify({
      data: [],
      columns: []
    });
    refreshTable(htmlServerMetricsTable, SERVER_METRICS);
    $(htmlServerMetricsBannerMessage).text('ERROR: missing server metrics parameters.');
    $(htmlServerMetricsBanner).show();
    return;
  }

  getServerMetrics(serverMetricsType, serverMetricsResourceGroup, serverMetricsAddress).then(
    function () {
      refreshTable(htmlServerMetricsTable, SERVER_METRICS);
      $(htmlServerMetricsBanner).hide();
    }).fail(function () {
    sessionStorage[SERVER_METRICS] = JSON.stringify({
      data: [],
      columns: []
    });
    refreshTable(htmlServerMetricsTable, SERVER_METRICS);
    $(htmlServerMetricsBannerMessage).text('ERROR: unable to retrieve server metrics.');
    $(htmlServerMetricsBanner).show();
  });
}

function refresh() {
  refreshServerMetrics();
}

$(function () {
  sessionStorage[SERVER_METRICS] = JSON.stringify({
    data: [],
    columns: []
  });
  refreshServerMetrics();
});
