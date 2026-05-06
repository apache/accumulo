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
          <div class="deployment-overview-content">
            <div class="mb-3">
              <label for="deployment-rg-filter" class="form-label">Resource Group Filter</label>
              <input type="text" id="deployment-rg-filter" class="form-control"
                placeholder="Enter resource group regex">
              <small id="deployment-rg-feedback" class="form-text text-danger"
                style="display:none;">Invalid regex pattern</small>
            </div>
            <div id="deploymentBreakdownMatrix"></div>
          </div>
        </div>
      </div>
