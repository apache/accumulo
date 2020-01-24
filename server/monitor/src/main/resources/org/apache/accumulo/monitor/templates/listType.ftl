<#--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->
      <script>
        /**
         * Creates initial trace list type table, and passes type and minutes values from template
         */
        $(document).ready(function() {
          type = '${type}';
          minutes = '${minutes}';
          refreshListType();
        });
      </script>

      <div class="row">
        <div class="col-xs-12">
          <h3>${title}</h3>
        </div>
      </div>
      <div class="row">
        <div class="col-xs-12">
          <table id="trace" class="table table-bordered table-striped table-condensed">
            <caption><span class="table-caption">Traces for masterReplicationDriver</span><br/></caption>
            <thead>
              <tr>
                <th class="firstcell" title="Start Time of selected trace type">Start</th>
                <th title="Span Time of selected trace type">ms</th>
                <th title="Service and Location of selected trace type">Source</th>
              </tr>
            </thead>
            <tbody></tbody>
          </table>
        </div>
      </div>
