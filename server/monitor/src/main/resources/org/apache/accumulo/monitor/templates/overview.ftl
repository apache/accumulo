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
        <div class="col-sm-6 col-sm-offset-3" id="manager">
          <table class="table table-bordered table-striped table-condensed">
            <thead>
              <tr>
                <th colspan="2"><a href="/manager">Accumulo Manager</a></th>
              </tr>
              <tr>
                <td colspan="2" class="center" style="display:none;"><span class="label label-danger nowrap">Manager is Down</span></td>
              </tr>
              <tr>
                <td class="left"><a href="/tables">Tables</a></td>
                <td class="right"></td>
              </tr>
              <tr>
                <td class="left"><a href="/tservers">Total&nbsp;Known&nbsp;Tablet&nbsp;Servers</a></td>
                <td class="right"></td>
              </tr>
              <tr>
                <td class="left"><a href="/tservers">Dead&nbsp;Tablet&nbsp;Servers</a></td>
                <td class="right"></td>
              </tr>
              <tr>
                <td class="left">Tablets</td>
                <td class="right"></td>
              </tr>
              <tr>
                <td class="left">Entries</td>
                <td class="right"></td>
              </tr>
              <tr>
                <td class="left">Lookups</td>
                <td class="right"></td>
              </tr>
              <tr>
                <td class="left">Uptime</td>
                <td class="right"></td>
              </tr>
            </thead>
            <tbody></tbody>
          </table>
        </div>
      </div>
      <div class="row">
        <div class="col-sm-6">
          <div class="plotHeading">Ingest (Entries/s)</div>
          <div id="ingest_entries" class="plot"></div>
        </div>
        <div class="col-sm-6">
          <div class="plotHeading">Scan (Entries/s)</div>
          <div id="scan_entries" class="plot"></div>
        </div>
      </div>
      <div class="row">
        <div class="col-sm-6">
          <div class="plotHeading">Ingest (MB/s)</div>
          <div id="ingest_mb" class="plot"></div>
        </div>
        <div class="col-sm-6">
          <div class="plotHeading">Scan (MB/s)</div>
          <div id="scan_mb" class="plot"></div>
        </div>
      </div>
      <div class="row">
        <div class="col-sm-6">
          <div class="plotHeading">Load Average</div>
          <div id="load_avg" class="plot"></div>
        </div>
        <div class="col-sm-6">
          <div class="plotHeading">Seeks</div>
          <div id="seeks" class="plot"></div>
        </div>
      </div>
      <div class="row">
        <div class="col-sm-6">
          <div class="plotHeading">Minor Compactions</div>
          <div id="minor" class="plot"></div>
        </div>
        <div class="col-sm-6">
          <div class="plotHeading">Major Compactions</div>
          <div id="major" class="plot"></div>
        </div>
      </div>
      <div class="row">
        <div class="col-sm-6">
          <div class="plotHeading">Index Cache Hit Rate</div>
          <div id="index_cache" class="plot"></div>
        </div>
        <div class="col-sm-6">
          <div class="plotHeading">Data Cache Hit Rate</div>
          <div id="data_cache" class="plot"></div>
        </div>
      </div>
