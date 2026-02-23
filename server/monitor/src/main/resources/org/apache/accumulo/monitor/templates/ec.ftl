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
      <div class="row">
        <div class="col-xs-12">
          <h3>${title}</h3>
        </div>
      </div>
      <div id="ecDiv" style="display: none;">
        <div class="row">
          <div class="col-xs-12">
            <table id="coordinatorTable" class="table caption-top table-bordered table-striped table-condensed">
              <caption><span class="table-caption">Compaction&nbsp;Coordinator</span></caption>
              <thead>
                <tr>
                  <th class="firstcell" title="The hostname the compactor coordinator is running on.">Server&nbsp;</th>
                  <th title="Number of queues configured">Queues</th>
                  <th title="Number of compactors running">Compactors</th>
                  <th class="duration">Last&nbsp;Contact</th>
                </tr>
              </thead>
            </table>
          </div>
        </div>
        <br />
        <div class="row">
          <div class="col-xs-12">
            <table id="compactorsTable" class="table caption-top table-bordered table-striped table-condensed">
              <caption><span class="table-caption">Compactors</span>&nbsp;&nbsp;
                <a href="javascript:refreshCompactors();"><span style="font-size: 1.5em; color: black;" class="bi bi-arrow-repeat"></span></a>
              </caption>
              <thead>
                <tr>
                  <th class="firstcell" title="The hostname the compactor is running on.">Server</th>
                  <th title="The name of the group this compactor is assigned.">Group</th>
                  <th class="duration" title="Last time data was fetched. Server fetches on refresh, at most every minute.">Last Contact</th>
                </tr>
              </thead>
            </table>
          </div>
        </div>
        <br />
        <div class="row">
          <div class="col-xs-12">
            <table id="runningTable" class="table caption-top table-bordered table-striped table-condensed">
              <caption>
                <div class="d-flex justify-content-between align-items-center mb-3">
                  <div>
                    <span class="table-caption">Running Compactions</span>&nbsp;&nbsp;
                    <a href="javascript:refreshRunning();">
                      <span style="font-size: 1.5em; color: black;" class="bi bi-arrow-repeat"></span>
                    </a>
                  </div>
                </div>
                <div class="accordion" id="filterAccordion">
                  <div class="accordion-item">
                    <h2 class="accordion-header" id="filterHeading">
                      <button class="accordion-button collapsed" type="button" data-bs-toggle="collapse" data-bs-target="#filterCollapse" aria-expanded="false" aria-controls="filterCollapse">
                        Filters
                      </button>
                    </h2>
                    <div id="filterCollapse" class="accordion-collapse collapse" aria-labelledby="filterHeading" data-bs-parent="#filterAccordion">
                      <div class="accordion-body">
                        <!-- Hostname Filter -->
                        <div class="mb-3">
                          <label for="hostname-filter" class="form-label">Hostname Filter</label>
                          <input type="text" id="hostname-filter" class="form-control" placeholder="Enter hostname regex">
                          <small id="hostname-feedback" class="form-text text-danger" style="display:none;">Invalid regex pattern</small>
                        </div>
                        <!-- Queue Filter -->
                        <div class="mb-3">
                          <label for="queue-filter" class="form-label">Queue Filter</label>
                          <input type="text" id="queue-filter" class="form-control" placeholder="Enter queue regex">
                          <small id="queue-feedback" class="form-text text-danger" style="display:none;">Invalid regex pattern</small>
                        </div>
                        <!-- Table ID Filter -->
                        <div class="mb-3">
                          <label for="tableid-filter" class="form-label">Table ID Filter</label>
                          <input type="text" id="tableid-filter" class="form-control" placeholder="Enter table ID regex">
                          <small id="tableid-feedback" class="form-text text-danger" style="display:none;">Invalid regex pattern</small>
                        </div>
                        <!-- ECID Filter -->
                        <div class="mb-3">
                          <label for="ecid-filter" class="form-label">ECID Filter</label>
                          <input type="text" id="ecid-filter" class="form-control" placeholder="Enter ECID regex">
                          <small id="ecid-feedback" class="form-text text-danger" style="display:none;">Invalid regex pattern</small>
                        </div>
                        <!-- Duration Filter -->
                        <div class="mb-3">
                          <label for="duration-filter" class="form-label">Duration Filter</label>
                          <input type="text" id="duration-filter" class="form-control" placeholder="Enter duration (e.g., &gt;10m, &lt;1h, &gt;=5s, &lt;=2d)">
                          <small id="duration-feedback" class="form-text text-danger" style="display:none;">Invalid duration format</small>
                          <small class="form-text text-muted">Valid formats: &gt;10m, &lt;1h, &gt;=5s, &lt;=2d</small>
                        </div>
                        <!-- Clear Filters Button -->
                        <div class="mb-3">
                          <button id="clear-filters" class="btn btn-secondary">Clear Filters</button>
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              </caption>
              <thead>
                <tr>
                  <th class="firstcell" title="The hostname the compactor is running on.">Server Hostname</th>
                  <th title="The type of compaction.">Kind</th>
                  <th title="The status returned by the last update.">Status</th>
                  <th title="The name of the queue this compactor is assigned.">Queue</th>
                  <th title="The ID of the table being compacted.">Table ID</th>
                  <th title="The ID of the running external compaction.">ECID</th>
                  <th title="The number of files being compacted."># of Files</th>
                  <th title="The progress of the compaction." class="progBar">Progress</th>
                  <th class="duration" title="The time of the last update for the compaction">Last Update</th>
                  <th class="duration" title="How long compaction has been running">Duration</th>
                  <th class="details-control">More</th>
                </tr>
              </thead>
              <tbody></tbody>
            </table>
          </div>
        </div>
      </div>
    <div id="ccBanner" style="display: none;">
      <div class="alert alert-danger" role="alert">Compaction Coordinator Not Running</div>
    </div>
