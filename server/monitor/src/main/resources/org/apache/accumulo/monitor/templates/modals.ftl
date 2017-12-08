<#--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
    <!-- Modal -->
    <div id="aboutModal" data-target="#aboutModal" class="modal fade" role="dialog">
      <div class="modal-dialog" role="document">
        <!-- Modal content -->
        <div class="modal-content">
          <div class="modal-header">
            <button type="button" class="close" data-dismiss="modal">&times;</button>
            <span class="modal-title"><a href="https://accumulo.apache.org" target="_blank"><img alt="Apache Accumulo" src="/resources/images/accumulo-logo.png" /></a></span>
          </div>
          <div class="modal-body">
            <div class="container-fluid">
              <div class="row">
                <div class="col-sm-4 text-right">Software</div>
                <div class="col-sm-6 text-left"><a href="https://accumulo.apache.org" target="_blank">Apache&nbsp;Accumulo</a></div>
              </div>
              <div class="row">
                <div class="col-sm-4 text-right">Version</div>
                <div class="col-sm-6 text-left">${version}</div>
              </div>
              <div class="row">
                <div class="col-sm-4 text-right">Instance&nbsp;Name</div>
                <div class="col-sm-6 text-left">${instance_name}</div>
              </div>
              <div class="row">
                <div class="col-sm-4 text-right">Instance&nbsp;Id</div>
                <div class="col-sm-6 text-left">${instance_id}</div>
              </div>
            </div>
          </div>
          <div class="modal-footer">
            <button type="button" class="btn btn-default" data-dismiss="modal">Close</button>
          </div>
        </div>
      </div>
    </div>
