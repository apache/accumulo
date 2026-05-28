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
        <div class="col-xs-12">
          <div class="row mb-4">
            <div id="deploymentWarning"></div>
            <div class="col">
              <div class="card">
                <div class="card-header fw-semibold">Server Deployment</div>
                <div class="card-body">
                  <div class="deployment-overview-content">
                    <div class="table-responsive">
                      <table id="deployment-table" class="table table-bordered table-sm align-middle deployment-matrix-table mb-0">
                        <#include "table_loading.ftl" >
                      </table>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>

          <div class="row row-cols-1 row-cols-lg-2 row-cols-xxl-4 g-3 mb-4">
            <div class="col">
              <div class="card h-100">
                <div class="card-header fw-semibold">Instance</div>
                <ul id="instance-overview-list" class="list-group list-group-flush">
                  <li class="list-group-item text-center">
                    <div class="spinner-border spinner-border-sm" role="status">
                      <span class="visually-hidden">Loading...</span>
                    </div>
                    <span>Loading...</span>
                  </li>
                </ul>
              </div>
            </div>
            <div class="col">
              <div class="card h-100">
                <div class="card-header fw-semibold">Ingest</div>
                <ul id="ingest-overview-list" class="list-group list-group-flush">
                  <li class="list-group-item text-center">
                    <div class="spinner-border spinner-border-sm" role="status">
                      <span class="visually-hidden">Loading...</span>
                    </div>
                    <span>Loading...</span>
                  </li>
                </ul>
              </div>
            </div>
            <div class="col">
              <div class="card h-100">
                <div class="card-header fw-semibold">Scan</div>
                <ul id="scan-overview-list" class="list-group list-group-flush">
                  <li class="list-group-item text-center">
                    <div class="spinner-border spinner-border-sm" role="status">
                      <span class="visually-hidden">Loading...</span>
                    </div>
                    <span>Loading...</span>
                  </li>
                </ul>
              </div>
            </div>
            <div class="col">
              <div class="card h-100">
                <div class="card-header fw-semibold">Compaction</div>
                <ul id="compaction-overview-list" class="list-group list-group-flush">
                  <li class="list-group-item text-center">
                    <div class="spinner-border spinner-border-sm" role="status">
                      <span class="visually-hidden">Loading...</span>
                    </div>
                    <span>Loading...</span>
                  </li>
                </ul>
              </div>
            </div>
          </div>
        </div>
      </div>
