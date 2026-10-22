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
    <nav class="navbar navbar-expand-lg bg-body-tertiary border-bottom">
      <div class="container-fluid">
        <a class="navbar-brand" id="headertitle" style="text-decoration: none" href="${rootContext}">
          <img id="accumulo-avatar" alt="accumulo" class="me-2" src="resources/images/accumulo-avatar.png" />
          ${instance_name?html}
        </a>
        <button class="navbar-toggler ms-auto" type="button" data-bs-toggle="collapse" data-bs-target="#nav-items" aria-controls="nav-items" aria-expanded="false" aria-label="Toggle navigation">
          <span class="navbar-toggler-icon"></span>
        </button>
        <div class="collapse navbar-collapse" id="nav-items">
          <ul class="navbar-nav ms-auto mb-2 mb-lg-0">
            <li class="navbar-text px-2">
              <button type="button" id="alertStatus" class="bi bi-exclamation-triangle-fill btn btn-link p-0 text-danger text-decoration-none" style="display: none; cursor: pointer;" title="System-wide alert. Click to display (if not already present)" aria-label="Show system-wide alert details"></button>
            </li>
            <li class="nav-item dropdown">
              <a class="nav-link dropdown-toggle" href="#" id="serversDropdown" role="button" data-bs-toggle="dropdown" aria-expanded="false">
                <span id="statusNotification" class="icon-dot normal"></span>&nbsp;Servers
              </a>
              <ul class="dropdown-menu" aria-labelledby="serversDropdown">
                <li><a class="link-body-emphasis dropdown-item d-flex justify-content-between align-items-center gap-3" href="compactors"><span><span id="compactorStatusNotification" class="icon-dot normal"></span>&nbsp;Compactors</span><span id="compactorStatusCount" class="server-count"></span></a></li>
                <li><a class="link-body-emphasis dropdown-item d-flex justify-content-between align-items-center gap-3" href="gc"><span><span id="gcStatusNotification" class="icon-dot normal"></span>&nbsp;Garbage&nbsp;Collector</span><span id="gcStatusCount" class="server-count"></span></a></li>
                <li><a class="link-body-emphasis dropdown-item d-flex justify-content-between align-items-center gap-3" href="manager"><span><span id="managerStatusNotification" class="icon-dot normal"></span>&nbsp;Managers</span><span id="managerStatusCount" class="server-count"></span></a></li>
                <li><a class="link-body-emphasis dropdown-item d-flex justify-content-between align-items-center gap-3" href="sservers"><span><span id="sserverStatusNotification" class="icon-dot normal"></span>&nbsp;Scan&nbsp;Servers</span><span id="sserverStatusCount" class="server-count"></span></a></li>
                <li><a class="link-body-emphasis dropdown-item d-flex justify-content-between align-items-center gap-3" href="tservers"><span><span id="serverStatusNotification" class="icon-dot normal"></span>&nbsp;Tablet&nbsp;Servers</span><span id="serverStatusCount" class="server-count"></span></a></li>
              </ul>
            </li>
            <li class="nav-item">
              <a class="nav-link" href="tables">Tables</a>
            </li>
            <li class="nav-item dropdown">
              <a class="nav-link dropdown-toggle" href="#" id="activityDropdown" role="button" data-bs-toggle="dropdown" aria-expanded="false">
                Activity
              </a>
              <ul class="dropdown-menu dropdown-menu-end" aria-labelledby="activityDropdown">
                <li><a class="link-body-emphasis dropdown-item" href="coordinator">Compaction Overview</a></li>
                <li><a class="link-body-emphasis dropdown-item" href="ec">Compaction Details</a></li>
                <li><a class="link-body-emphasis dropdown-item" href="fate">Fate Tx Details</a></li>
                <li><a class="link-body-emphasis dropdown-item" href="scans">Scans</a></li>
                <li><a class="link-body-emphasis dropdown-item" href="recovery">Tablet Recoveries</a></li>
              </ul>
            </li>
            <li class="nav-item">
              <a id="alert-anchor" class="nav-link" href="alerts"><span class="bi bi-exclamation-triangle-fill" aria-hidden="true"></span>&nbsp;Alerts</a>
            </li>
            <li class="nav-item dropdown">
              <a class="nav-link" href="#" id="preferencesDropdown" role="button" data-bs-toggle="dropdown" aria-expanded="false" aria-label="Preferences">
                <span style="font-size: 1.2em;" class="bi bi-gear"></span>
              </a>
              <ul class="dropdown-menu dropdown-menu-end" aria-labelledby="preferencesDropdown">
                <li><h6 class="dropdown-header fw-semibold pb-1">General Preferences</h6></li>
                <li>
                  <div class="dropdown-item d-flex justify-content-between align-items-center small">
                    <label class="form-check-label" for="darkThemeSwitch">Dark Theme</label>
                    <div class="form-check form-switch d-flex align-items-center mb-0 p-0 fs-6">
                      <input id="darkThemeSwitch" class="form-check-input float-none m-0" type="checkbox" role="switch">
                    </div>
                  </div>
                </li>
                <li>
                  <div class="dropdown-item d-flex justify-content-between align-items-center small">
                    <label class="form-check-label" for="autoRefreshSwitch">Auto-Refresh</label>
                    <div class="form-check form-switch d-flex align-items-center mb-0 p-0 fs-6">
                      <input id="autoRefreshSwitch" class="form-check-input float-none m-0" type="checkbox" role="switch">
                    </div>
                  </div>
                </li>
                <li><hr class="dropdown-divider"></li>
                <li><h6 class="dropdown-header fw-semibold pb-1">Alert Priorities</h6></li>
                <li>
                  <div class="dropdown-item d-flex justify-content-between align-items-center small">
                    <label class="form-check-label" for="alert-pri-switch-High">High</label>
                    <div class="form-check form-switch d-flex align-items-center mb-0 p-0 fs-6">
                      <input id="alert-pri-switch-High" class="form-check-input float-none m-0" type="checkbox" role="switch">
                    </div>
                  </div>
                </li>
                <li>
                  <div class="dropdown-item d-flex justify-content-between align-items-center small">
                    <label class="form-check-label" for="alert-pri-switch-Info">Info</label>
                    <div class="form-check form-switch d-flex align-items-center mb-0 p-0 fs-6">
                      <input id="alert-pri-switch-Info" class="form-check-input float-none m-0" type="checkbox" role="switch">
                    </div>
                  </div>
                </li>
                <li><hr class="dropdown-divider"></li>
                <li><h6 class="dropdown-header fw-semibold pb-1">Alert Categories</h6></li>
                <li>
                  <ul id="categories-list" class="list-unstyled mb-0">
                  </ul>
                </li>
                <li><hr class="dropdown-divider"></li>
                <li><a class="link-body-emphasis dropdown-item" data-bs-toggle="modal" href="#aboutModal">About</a></li>
              </ul>
            </li>
          </ul>
        </div>
      </div>
    </nav>
