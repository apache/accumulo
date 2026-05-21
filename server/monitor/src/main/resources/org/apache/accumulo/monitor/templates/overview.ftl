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
          <div class="row gx-4">
            <div class="col-2"></div>
            <div class="col-2"></div>
            <div class="col-2">
              <table id="instance-overview-table" class="table caption-top table-bordered table-striped table-condensed deployment-overview-content">
                <caption><span>Instance Overview</span></caption>
                <#include "table_loading.ftl" >
              </table>
              <br />
              <table id="compaction-overview-table" class="table caption-top table-bordered table-striped table-condensed deployment-overview-content">
                <caption><span>Compaction Overview</span></caption>
                <#include "table_loading.ftl" >
              </table>
            </div>
            <div class="col-2">
              <table id="ingest-overview-table" class="table caption-top table-bordered table-striped table-condensed deployment-overview-content">
                <caption><span>Ingest Overview</span></caption>
                <#include "table_loading.ftl" >
              </table>
               <br />
              <table id="scan-overview-table" class="table caption-top table-bordered table-striped table-condensed deployment-overview-content">
                <caption><span>Scan Overview</span></caption>
                <#include "table_loading.ftl" >
              </table>
            </div>
            <div class="col-2"></div>
            <div class="col-2"></div>
          </div>
          <br />
          <div class="row">
            <div id="deploymentWarning"></div>
            <div class="deployment-overview-content">
              <div class="table-responsive">
                <table id="deployment-table" class="table table-bordered table-sm align-middle deployment-matrix-table mb-0">
                  <caption><span class="table-caption">Server Deployment Overview</span><br />
                  <#include "table_loading.ftl" >
                </table>
              </div>
            </div>
          </div>
        </div>
      </div>
