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
    <!-- Modal -->
    <div id="aboutModal" data-target="#aboutModal" class="modal fade" role="dialog">
      <div class="modal-dialog" role="document">
        <!-- Modal content -->
        <div class="modal-content">
          <div>
            <div class="modal-header justify-content-center">
              <span class="modal-title">
                <div class="text-center">
                  <a href="https://accumulo.apache.org" target="_blank"><img alt="Apache Accumulo" src="/resources/images/accumulo-logo.png" /></a>
              </span>
            </div>
          </div>
          <div class="modal-body">
            <div class="container-fluid">
              <div class="row justify-content-evenly">
                <div class="col-sm-4 text-end">Software</div>
                <div class="col-sm-8 text-start"><a href="https://accumulo.apache.org" target="_blank">Apache&nbsp;Accumulo</a></div>
              </div>
              <div class="row justify-content-evenly">
                <div class="col-sm-4 text-end">Version</div>
                <div class="col-sm-8 text-start">${version}</div>
              </div>
              <div class="row justify-content-evenly">
                <div class="col-sm-4 text-end">Instance&nbsp;Name</div>
                <div class="col-sm-8 text-start">${instance_name}</div>
              </div>
              <div class="row justify-content-evenly">
                <div class="col-sm-4 text-end">Instance&nbsp;Id</div>
                <div class="col-sm-8 text-start">${instance_id}</div>
              </div>
              <div class="row justify-content-evenly">
                <div class="col-sm-4 text-end">ZooKeeper&nbsp;Hosts</div>
                <div class="col-sm-8 text-start">${zk_hosts}</div>
              </div>
            </div>
          </div>
          <div class="modal-footer">
            <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
          </div>
        </div>
      </div>
    </div>
    