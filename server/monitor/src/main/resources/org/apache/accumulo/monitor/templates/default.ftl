<#--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->
<!DOCTYPE html>
<html>
  <head>
    <title>${title} - Accumulo ${version}</title>
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
    <!-- external resources configurable by setting monitor.resources.external -->
    <!-- make sure jquery is included first - other scripts depend on it -->
    <#if externalResources?has_content>
      <#list externalResources as val>
        ${val}
      </#list>
    <#else>
      <script src="/resources/external/jquery/jquery-3.6.1.js"></script>
      <script src="/resources/external/bootstrap/js/bootstrap.js"></script>
      <script src="/resources/external/datatables/js/jquery.dataTables.js"></script>
      <script src="/resources/external/datatables/js/dataTables.bootstrap5.js"></script>
      <script src="/resources/external/flot/jquery.canvaswrapper.js"></script>
      <script src="/resources/external/flot/jquery.colorhelpers.js"></script>
      <script src="/resources/external/flot/jquery.flot.js"></script>
      <script src="/resources/external/flot/jquery.flot.saturated.js"></script>
      <script src="/resources/external/flot/jquery.flot.browser.js"></script>
      <script src="/resources/external/flot/jquery.flot.drawSeries.js"></script>
      <script src="/resources/external/flot/jquery.flot.uiConstants.js"></script>
      <script src="/resources/external/flot/jquery.flot.legend.js"></script>
      <script src="/resources/external/flot/jquery.flot.time.js"></script>
      <script src="/resources/external/flot/jquery.flot.resize.js"></script>
      <link rel="stylesheet" href="/resources/external/bootstrap/css/bootstrap.css" />
      <link rel="stylesheet" href="/resources/external/datatables/css/dataTables.bootstrap5.css" />
    </#if>

    <!-- accumulo resources -->
    <link rel="shortcut icon" type="image/jng" href="/resources/images/favicon.png" />
    <script src="/resources/js/global.js"></script>
    <script src="/resources/js/functions.js"></script>
    <link rel="stylesheet" type="text/css" href="/resources/css/screen.css" media="screen" />

    <script>
      /**
       * Sets up autorefresh on initial load
       */
      $(document).ready(function() {
        setupAutoRefresh();
      });
    </script>
    <#if js??>
      <script src="/resources/js/${js}"></script>
    </#if>
    <script src="/resources/js/navbar.js"></script>
  </head>

  <body>
    <#include "navbar.ftl">

    <div id="main" class="container-fluid">
      <#include "${template}">
    </div>

    <#include "modals.ftl">
  </body>
</html>
