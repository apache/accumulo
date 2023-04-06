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
      <div class="row">
        <div class="col-xs-12">
          <table id="bulkListTable" class="table caption-top table-bordered table-striped table-condensed">
            <caption><span class="table-caption">Bulk Imports</span><br /></caption>
            <thead>
              <tr>
                <th>Directory&nbsp;</th>
                <th title="The age of the import.">Age&nbsp;</th>
                <th title="The current state of the bulk import">State&nbsp;</th>
              </tr>
            </thead>
            <tbody></tbody>
          </table>
        </div>
      </div>
      <br /><br />
      <div class="row">
        <div class="col-xs-12">
          <table id="bulkPerServerTable" class="table caption-top table-bordered table-striped table-condensed">
            <caption><span class="table-caption">Per TabletServer</span><br /></caption>
            <thead>
              <tr>
                <th>Server</th>
                <th title="Number of imports presently running">#</th>
                <th title="The age of the oldest import running on this server.">Oldest&nbsp;Age</th>
              </tr>
            </thead>
            <tbody></tbody>
          </table>
        </div>
