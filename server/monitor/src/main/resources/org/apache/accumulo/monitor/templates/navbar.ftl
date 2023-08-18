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
                <span id="statusNotification" class="status-icon normal">
                  <svg id="statusNotificationDot" xmlns="http://www.w3.org/2000/svg" width="16" height="16" class="bi bi-circle-fill" viewBox="0 0 16 16">
                    <circle cx="8" cy="8" r="8"/>
                  </svg>
                  <svg id="statusNotificationCone" style="display:none;" xmlns="http://www.w3.org/2000/svg" width="16" height="16" class="bi bi-cone-striped" viewBox="0 0 16 16">
                    <path d="m9.97 4.88.953 3.811C10.159 8.878 9.14 9 8 9c-1.14 0-2.158-.122-2.923-.309L6.03 4.88C6.635 4.957 7.3 5 8 5s1.365-.043 1.97-.12zm-.245-.978L8.97.88C8.718-.13 7.282-.13 7.03.88L6.275 3.9C6.8 3.965 7.382 4 8 4c.618 0 1.2-.036 1.725-.098zm4.396 8.613a.5.5 0 0 1 .037.96l-6 2a.5.5 0 0 1-.316 0l-6-2a.5.5 0 0 1 .037-.96l2.391-.598.565-2.257c.862.212 1.964.339 3.165.339s2.303-.127 3.165-.339l.565 2.257 2.391.598z"/>
                  </svg>
                </span>
                &nbsp;Servers
              </a>
              <ul class="dropdown-menu">
                <li>
                  <a class="dropdown-item" href="/manager">
                    <div id="managerStatusIcon" class="status-icon normal">
                      <div id="managerStatusDot">
                        <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" class="bi bi-circle-fill" viewBox="0 0 16 16">
                          <circle cx="8" cy="8" r="8"/>
                        </svg>
                      </div>
                      <div id="managerStatusCone" style="display: none;">
                        <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" class="bi bi-cone-striped" viewBox="0 0 16 16">
                          <path d="m9.97 4.88.953 3.811C10.159 8.878 9.14 9 8 9c-1.14 0-2.158-.122-2.923-.309L6.03 4.88C6.635 4.957 7.3 5 8 5s1.365-.043 1.97-.12zm-.245-.978L8.97.88C8.718-.13 7.282-.13 7.03.88L6.275 3.9C6.8 3.965 7.382 4 8 4c.618 0 1.2-.036 1.725-.098zm4.396 8.613a.5.5 0 0 1 .037.96l-6 2a.5.5 0 0 1-.316 0l-6-2a.5.5 0 0 1 .037-.96l2.391-.598.565-2.257c.862.212 1.964.339 3.165.339s2.303-.127 3.165-.339l.565 2.257 2.391.598z"/>
                        </svg>
                      </div>
                      &nbsp;Manager&nbsp;Server&nbsp;
                    </div>
                  </a>
                </li>
                <li>
                  <a class="dropdown-item" href="/tservers">
                    <div id="serverStatusNotification" class="status-icon normal">
                      <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" class="bi bi-circle-fill" viewBox="0 0 16 16">
                        <circle cx="8" cy="8" r="8"/>
                      </svg>
                    </div>
                    &nbsp;Tablet&nbsp;Servers&nbsp;
                  </a>
                </li>
                <li>
                  <a class="dropdown-item" href="/gc">
                    <div id="gcStatusNotification" class="status-icon normal">
                      <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" class="bi bi-circle-fill" viewBox="0 0 16 16">
                        <circle cx="8" cy="8" r="8"/>
                      </svg>
                    </div>
                    &nbsp;Garbage&nbsp;collector&nbsp;
                  </a>
                </li>
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
                <li><a class="dropdown-item" href="/replication">Replication</a></li>
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