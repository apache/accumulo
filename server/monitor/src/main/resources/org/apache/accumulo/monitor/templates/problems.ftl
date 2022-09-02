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
      <script>
        /**
         * Creates problems initial table, passes tableID from template
         */
        $(document).ready(function() {
          tableID = <#if table??>'${table}'<#else>''</#if>;
          refreshProblems();
        });
      </script>
      <div class="row">
        <div class="col-xs-12">
          <h3>${title}</h3>
        </div>
      </div>
      <div class="row">
        <div class="col-xs-12">
          <table id="problemSummary" class="table caption-top table-bordered table-striped table-condensed">
            <caption><span class="table-caption">Summary</span></caption>
            <thead>
              <tr>
                <th class="firstcell">Table&nbsp;</th>
                <th class="big-num">FILE_READ&nbsp;</th>
                <th class="big-num">FILE_WRITE&nbsp;</th>
                <th class="big-num">TABLET_LOAD&nbsp;</th>
                <th>Operations&nbsp;</th>
              </tr>
            </thead>
            <tbody></tbody>
          </table>
        </div>
      </div>
      <br />
      <div class="row">
        <div class="col-xs-12">
          <table id="problemDetails" class="table caption-top table-bordered table-striped table-condensed">
            <caption><span class="table-caption">Details</span></caption>
            <thead>
              <tr>
                <th class="firstcell">Table&nbsp;</th>
                <th>Problem&nbsp;Type&nbsp;</th>
                <th>Server&nbsp;</th>
                <th class="date">Time&nbsp;</th>
                <th>Resource&nbsp;</th>
                <th>Exception&nbsp;</th>
                <th>Operations&nbsp;</th>
              </tr>
            </thead>
            <tbody></tbody>
          </table>
        </div>
      </div>
