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
        <div class="col-12">
          <h1 class="h3">${title}</h1>
        </div>
      </div>
      <div id="runningDiv">
        <div class="row">
          <div class="col-12">
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
                      <input type="text" id="hostname-filter" class="form-control" placeholder="Enter hostname regex" aria-describedby="hostname-feedback">
                      <small id="hostname-feedback" class="form-text text-danger" style="display:none;" aria-live="polite">Invalid regex pattern</small>
                    </div>
                    <!-- Queue Filter -->
                    <div class="mb-3">
                      <label for="queue-filter" class="form-label">Queue Filter</label>
                      <input type="text" id="queue-filter" class="form-control" placeholder="Enter queue regex" aria-describedby="queue-feedback">
                      <small id="queue-feedback" class="form-text text-danger" style="display:none;" aria-live="polite">Invalid regex pattern</small>
                    </div>
                    <!-- Table ID Filter -->
                    <div class="mb-3">
                      <label for="tableid-filter" class="form-label">Table ID Filter</label>
                      <input type="text" id="tableid-filter" class="form-control" placeholder="Enter table ID regex" aria-describedby="tableid-feedback">
                      <small id="tableid-feedback" class="form-text text-danger" style="display:none;" aria-live="polite">Invalid regex pattern</small>
                    </div>
                    <!-- ECID Filter -->
                    <div class="mb-3">
                      <label for="ecid-filter" class="form-label">ECID Filter</label>
                      <input type="text" id="ecid-filter" class="form-control" placeholder="Enter ECID regex" aria-describedby="ecid-feedback">
                      <small id="ecid-feedback" class="form-text text-danger" style="display:none;" aria-live="polite">Invalid regex pattern</small>
                    </div>
                    <!-- Duration Filter -->
                    <div class="mb-3">
                      <label for="duration-filter" class="form-label">Duration Filter</label>
                      <input type="text" id="duration-filter" class="form-control" placeholder="Enter duration (e.g., &gt;10m, &lt;1h, &gt;=5s, &lt;=2d)" aria-describedby="duration-feedback duration-help">
                      <small id="duration-feedback" class="form-text text-danger" style="display:none;" aria-live="polite">Invalid duration format</small>
                      <small id="duration-help" class="form-text text-body-secondary">Valid formats: &gt;10m, &lt;1h, &gt;=5s, &lt;=2d</small>
                    </div>
                    <!-- Clear Filters Button -->
                    <div class="mb-3">
                      <button id="clear-filters" type="button" class="btn btn-secondary">Clear Filters</button>
                    </div>
                  </div>
                </div>
              </div>
            </div>
            <table id="runningTable" class="table caption-top table-bordered table-striped table-sm">
              <caption>Running External Compactions</caption>
              <thead>
                <tr>
                  <th class="firstcell" title="The hostname the compactor is running on.">Server Hostname</th>
                  <th title="The type of compaction.">Kind</th>
                  <th title="The status returned by the last update.">Status</th>
                  <th title="The name of the queue this compactor is assigned.">Queue</th>
                  <th title="The ID of the table being compacted.">Table Name</th>
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
    <div id="managerBanner" style="display: none;">
      <div class="alert alert-danger" role="alert">Manager Not Running</div>
    </div>
