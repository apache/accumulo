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
        <div class="col-xs-12 center nowrap">
          <h3>${title}</h3>
        </div>
      </div>
      <div class="row d-flex justify-content-center">
        <div class="col-xs-12" id="deploymentOverview">
          <div id="deploymentWarning"></div>
          <div class="mb-4" style="max-width: 560px; margin: 0 auto;">
            <table id="deploymentSummaryTable"
              class="table table-bordered table-striped table-condensed" style="width: 100%;">
              <thead>
                <tr>
                  <th colspan="2" class="center">Server Type Summary</th>
                </tr>
                <tr>
                  <th>Server Type</th>
                  <th>Responding / Total</th>
                </tr>
              </thead>
              <tbody></tbody>
            </table>
          </div>

          <div style="max-width: 760px; margin: 0 auto;">
            <div class="mb-3">
              <label for="deployment-rg-filter" class="form-label">Resource Group Filter</label>
              <input type="text" id="deployment-rg-filter" class="form-control"
                placeholder="Enter resource group regex">
              <small id="deployment-rg-feedback" class="form-text text-danger"
                style="display:none;">Invalid regex pattern</small>
            </div>
            <table id="deploymentBreakdownTable"
              class="table table-bordered table-striped table-condensed" style="width: 100%;">
              <thead>
                <tr>
                  <th colspan="3" class="center">Deployment Breakdown</th>
                </tr>
                <tr>
                  <th>Resource Group</th>
                  <th>Server Type</th>
                  <th>Responding / Total</th>
                </tr>
              </thead>
              <tbody></tbody>
            </table>
          </div>
        </div>
      </div>
