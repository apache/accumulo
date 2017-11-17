<#--
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
      <div class="container-fluid">
        <!-- toggle -->
        <div class="navbar-header">
          <button type="button" class="navbar-toggle collapsed" data-toggle="collapse" data-target="#nav-items" aria-expanded="false">
            <span class="sr-only">Toggle navigation</span>
            <span class="icon-bar"></span>
            <span class="icon-bar"></span>
            <span class="icon-bar"></span>
          </button>
          <a class="navbar-brand" id="headertitle" href="/">${instance_name}</a>
        </div>
        <!-- Nav links -->
        <div class="collapse navbar-collapse" id="nav-items">
          <ul class="nav navbar-nav navbar-right">
            <li class="dropdown">
              <a class="dropdown-toggle" data-toggle="dropdown" href="#" role="button" aria-haspopup="true" aria-expanded="false"><span id="statusNotification" class="icon-dot normal"></span>&nbsp;Servers&nbsp;<span class="caret"></span>
              </a>
              <ul class="dropdown-menu">
                <li><a href="/master"><span id="masterStatusNotification" class="icon-dot normal"></span>&nbsp;Master&nbsp;Server&nbsp;</a></li>
                <li><a href="/tservers"><span id="serverStatusNotification" class="icon-dot normal"></span>&nbsp;Tablet&nbsp;Servers&nbsp;</a></li>
                <li><a href="/gc"><span id="gcStatusNotification" class="icon-dot normal"></span>&nbsp;Garbage&nbsp;collector&nbsp;</a></li>
              </ul>
            </li>
            <li><a href="/tables">Tables</a></li>
            <li class="dropdown">
              <a class="dropdown-toggle" data-toggle="dropdown" href="#" role="button" aria-haspopup="true" aria-expanded="false">
                Activity <span class="caret"></span>
              </a>
              <ul class="dropdown-menu">
                <li><a href="/scans">Active&nbsp;Scans</a></li>
                <li><a href="/bulkImports">Bulk&nbsp;Imports</a></li>
                <li><a href="/vis">Server&nbsp;Activity</a></li>
                <li><a href="/replication">Replication</a></li>
              </ul>
            </li>
            <li class="dropdown">
              <a class="dropdown-toggle" data-toggle="dropdown" href="#" role="button" aria-haspopup="true" aria-expanded="false">Debug&nbsp;<span id="errorsNotification" class="badge"></span><span class="caret"></span>
              </a>
              <ul class="dropdown-menu">
                <li><a href="/trace/summary?minutes=10">Recent&nbsp;Traces</a></li>
                <li><a href="/log">Recent&nbsp;Logs&nbsp;<span id="recentLogsNotifications" class="badge"></span></a></li>
                <li><a href="/problems">Table&nbsp;Problems&nbsp;<span id="tableProblemsNotifications" class="badge"></span></a></li>
              </ul>
            </li>
            <li class="dropdown">
              <a class="dropdown-toggle" data-toggle="dropdown" href="#" role="button" aria-haspopup="true" aria-expanded="false">
                API <span class="caret"></span>
              </a>
              <ul class="dropdown-menu">
                <li><a href="/rest">XML</a></li>
                <li><a href="/rest/tservers">JSON</a></li>
              </ul>
            </li>
            <li class="dropdown">
              <a class="dropdown-toggle" data-toggle="dropdown" href="#" role="button" aria-haspopup="true" aria-expanded="false">
                <span class="glyphicon glyphicon-option-vertical"></span>
              </a>
              <ul class="dropdown-menu">
                <li><a class="auto-refresh" style="cursor:pointer">Auto-Refresh</a></li>
                <li><a data-toggle="modal" href="#aboutModal">About</a></li>
              </ul>
            </li>
          </ul>
        </div>
      </div>
      <script>
        // Obtain the current time
        document.getElementById('currentDate').innerHTML = Date();
      </script>
