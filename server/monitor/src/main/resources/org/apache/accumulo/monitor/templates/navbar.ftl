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
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark">
      <div class="container-fluid">
        <div class="navbar-header">
          <a class="navbar-brand" id="headertitle" style="text-decoration: none" href="/">
            <img id="accumulo-avatar" alt="accumulo" class="navbar-left" src="/resources/images/accumulo-avatar.png" />
            ${instance_name}
          </a>
          <button class="navbar-toggler" type="button" data-bs-toggle="collapse" data-bs-target="#nav-items" aria-controls="nav-items" aria-expanded="false" aria-label="Toggle navigation">
            <span class="navbar-toggler-icon"></span>
          </button>
        </div>
        <div class="collapse navbar-collapse" id="nav-items">
          <ul class="navbar-nav ms-auto mb-2 mb-lg-0">
            <li class="nav-item dropdown">
              <a class="nav-link dropdown-toggle" href="#" id="navbarDropdown" role="button" data-bs-toggle="dropdown" aria-expanded="false">
                <span id="statusNotification" class="icon-dot normal"></span>&nbspServers
              </a>
              <ul class="dropdown-menu">
                <li><a class="dropdown-item" href="/manager"><span id="managerStatusNotification" class="icon-dot normal"></span>&nbsp;Manager&nbsp;Server&nbsp;</a></li>
                <li><a class="dropdown-item" href="/tservers"><span id="serverStatusNotification" class="icon-dot normal"></span>&nbsp;Tablet&nbsp;Servers&nbsp;</a></li>
                <li><a class="dropdown-item" href="/gc"><span id="gcStatusNotification" class="icon-dot normal"></span>&nbsp;Garbage&nbsp;collector&nbsp;</a></li>
              </ul>
            </li>
            <li>
              <a class="nav-link" aria-current="page" href="/tables">Tables</a>
            </li>
            <li class="dropdown">
              <a class="nav-link dropdown-toggle" href="#" id="navbarDropdown" role="button" data-bs-toggle="dropdown" aria-expanded="false">
                Activity
              </a>
              <ul class="dropdown-menu col-xs-12" aria-labelledby="navbarDropdown">
                <li><a class="dropdown-item" href="/compactions">Active&nbsp;Compactions</a></li>
                <li><a class="dropdown-item" href="/scans">Active&nbsp;Scans</a></li>
                <li><a class="dropdown-item" href="/bulkImports">Bulk&nbsp;Imports</a></li>
                <li><a class="dropdown-item" href="/ec">External&nbsp;Compactions</a></li>
              </ul>
            </li>
            <li class="dropdown">
              <a class="nav-link dropdown-toggle" href="#" id="navbarDropdown"
              role="button" data-bs-toggle="dropdown" aria-expanded="false">Debug&nbsp;<span id="errorsNotification" class="badge"></span><span class="caret"></span>
              </a>
              <ul class="dropdown-menu">
                <li><a class="dropdown-item" href="/log">Recent&nbsp;Logs&nbsp;<span id="recentLogsNotifications" class="badge"></span></a></li>
                <li><a class="dropdown-item" href="/problems">Table&nbsp;Problems&nbsp;<span id="tableProblemsNotifications" class="badge"></span></a></li>
              </ul>
            </li>
            <li class="dropdown">
              <a class="nav-link dropdown-toggle" href="#" id="navbarDropdown"
              role="button" data-bs-toggle="dropdown" aria-expanded="false">REST
              </a>
              <ul class="dropdown-menu dropdown-menu-end">
                <li><a class="dropdown-item" href="/rest/xml">XML Summary</a></li>
                <li><a class="dropdown-item" href="/rest/json">JSON Summary</a></li>
              </ul>
            </li>
            <li class="dropdown">
              <a class="nav-link" href="#" id="navbarDropdown"
              role="button" data-bs-toggle="dropdown" aria-expanded="false">
                <img src="../resources/external/bootstrap/fonts/three-dots-vertical.svg">
              </a>
              <ul class="dropdown-menu dropdown-menu-end">
                <li><a class="dropdown-item auto-refresh" style="cursor:pointer">Auto-Refresh</a></li>
                <li><a class="dropdown-item" data-bs-toggle="modal" href="#aboutModal">About</a></li>
              </ul>
            </li>
          </ul>
        </div>
      </div>
    </nav>
