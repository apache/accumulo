<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
<html>
  <head>
    <title>${title} - Accumulo ${version}</title>
    <meta http-equiv="Content-Type" content="test/html" />
    <meta http-equiv="Content-Script-Type" content="text/javascript" />
    <meta http-equiv="Content-Style-Type" content="text/css" />
    <!-- external resources configurable by setting monitor.resources.external -->
    <!-- make sure jquery is included first - other scripts depend on it -->
    <#if externalResources?has_content>
      <#list externalResources as val>
        ${val}
      </#list>
    <#else>
      <script src="/resources/external/jquery-2.2.4.min.js"></script>
      <script src="/resources/external/bootstrap.min.js"></script>
      <script src="/resources/external/jquery-ui.js"></script>
      <script src="/resources/external/select2.min.js"></script>
      <link rel="stylesheet" href="/resources/external/bootstrap.min.css" />
      <link rel="stylesheet" href="/resources/external/bootstrap-theme.min.css" />
      <link rel="stylesheet" href="/resources/external/jquery-ui.css" />
      <link rel="stylesheet" href="/resources/external/select2.min.css" />
    </#if>

    <!-- accumulo resources -->
    <link rel="shortcut icon" type="image/jng" href="/resources/images/favicon.png" />
    <script src="/resources/js/global.js" type="text/javascript"></script>
    <script src="/resources/js/functions.js" type="text/javascript"></script>
    <link rel="stylesheet" type="text/css" href="/resources/css/screen.css" media="screen" />

    <!-- bundled flot resources -->
    <script language="javascript" type="text/javascript" src="/resources/external/flot/jquery.flot.js"></script>
    <script language="javascript" type="text/javascript" src="/resources/external/flot/jquery.flot.time.js"></script>

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
    <#include "/templates/modals.ftl">
    <div id="content-wrapper">
      <div id="content">
        <div id="navbar" class="navbar navbar-inverse navbar-fixed-top">
          <#include "/templates/navbar.ftl">
        </div>

        <div id="main">
          <#include "/templates/${template}">

        </div>
      </div>
    </div>
  </body>
</html>
