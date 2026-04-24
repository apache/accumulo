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
    <div id="tserversManagerBanner" style="display: none;">
      <div class="alert alert-danger" role="alert">Manager Not Running</div>
    </div>
    <div id="tserversWarnBanner" style="display: none;">
      <div class="alert alert-warning" role="alert">
        One or more Tablet Servers are unavailable or reported as bad.
      </div>
    </div>
    <div id="tserversErrorBanner" style="display: none;">
      <div class="alert alert-danger" role="alert">
        No Tablet Servers are currently responding.
      </div>
    </div>
    <div id="tserversStatusBanner" style="display: none;">
      <div id="tservers-banner-message" class="alert" role="alert"></div>
    </div>    
    <div class="row">
      <div class="col-xs-12">
        <span id="recovery-caption" style="background-color: gold; display: none;">Highlighted rows correspond to tservers in recovery mode.</span>
        <table id="tservers" class="table caption-top table-bordered table-striped table-condensed">
          <caption><span class="table-caption">Tablet Servers</span><br />
            <span class="table-subcaption">The following Tablet Servers reported status.</span><br />
          </caption>
	      <thead><tr><th /></tr></thead>
	      <tbody>
	        <tr>
	          <td>
	            <div class="d-flex align-items-center">
	              <strong role="status">Loading...</strong>
	              <div class="spinner-border ms-auto" aria-hidden="true"></div>
	            </div>
	          </td>
	        </tr>
	      </tbody>
        </table>
      </div>
    </div>
