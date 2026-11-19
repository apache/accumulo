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
    <!-- About modal -->
    <div id="aboutModal" class="modal fade" tabindex="-1" aria-labelledby="aboutModalLabel" aria-hidden="true">
      <div class="modal-dialog">
        <div class="modal-content">
          <div class="modal-header justify-content-center position-relative">
            <h5 class="modal-title" id="aboutModalLabel">
              <a href="https://accumulo.apache.org" target="_blank" rel="noopener noreferrer">
                <img alt="Apache Accumulo" src="resources/images/accumulo-logo.png" />
              </a>
            </h5>
            <button type="button" class="btn-close position-absolute end-0 me-3" data-bs-dismiss="modal" aria-label="Close"></button>
          </div>
          <div class="modal-body">
            <div class="container-fluid">
              <div class="row align-items-center">
                <div class="col-sm-4 text-sm-end">Software</div>
                <div class="col-sm-8"><a href="https://accumulo.apache.org" target="_blank" rel="noopener noreferrer">Apache&nbsp;Accumulo</a></div>
              </div>
              <div class="row align-items-center">
                <div class="col-sm-4 text-sm-end">Version</div>
                <div class="col-sm-8"><span id="modal-version"></span></div>
              </div>
              <div class="row align-items-center">
                <div class="col-sm-4 text-sm-end">Instance&nbsp;Name</div>
                <div class="col-sm-8"><span id="modal-instance-name"></span></div>
              </div>
              <div class="row align-items-center">
                <div class="col-sm-4 text-sm-end">Instance&nbsp;Id</div>
                <div class="col-sm-8"><span id="modal-instance-id"></span></div>
              </div>
              <div class="row align-items-center">
                <div class="col-sm-4 text-sm-end">ZooKeeper&nbsp;Hosts</div>
                <div class="col-sm-8"><span id="modal-zk-hosts"></span></div>
              </div>
              <div class="row align-items-center">
                <div class="col-sm-4 text-sm-end">Volumes</div>
                <div class="col-sm-8"><span id="modal-volumes"></span></div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
    