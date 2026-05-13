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
        <div class="col-xs-12" id="instance-info">
			<div class="card mx-auto collapse" style="width: 24rem;" id="instance-card">
			  <div class="card-header text-center">
			    <strong id="instance-name"></strong>
			  </div>			
			  <div class="card-body">
	            <div class="row justify-content-evenly">
	              <div class="col-sm-4 text-end">Volumes</div>
	              <div id="instance-volumes" class="col-sm-8 text-start"></div>
	            </div>
	            <div class="row justify-content-evenly">
	              <div class="col-sm-4 text-end">ZooKeepers</div>
	              <div id="instance-zookeepers" class="col-sm-8 text-start"></div>
	            </div>
	            <div class="row justify-content-evenly">
	              <div class="col-sm-4 text-end">Version</div>
	              <div id="instance-version" class="col-sm-8 text-start"></div>
	            </div>			  
			  </div>
			  <div class="card-footer text-center text-muted" id="instance-uuid">
			  </div>			  
			</div>        
        </div>
      </div>
      <br />
      <br />
      <div class="row d-flex justify-content-center">
        <div class="col-xs-12" id="deploymentOverview">
          <div id="deploymentWarning"></div>
          <div class="deployment-overview-content">
            <div class="table-responsive">
              <table id="deployment-table" class="table table-bordered table-sm align-middle deployment-matrix-table mb-0 fs-4">
                <#include "table_loading.ftl" >
              </table>
            </div>
          </div>
        </div>
      </div>
