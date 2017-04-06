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
      <div><h3>${title}</h3></div>
      <br>
      <div class="center-block">
        <div class="container">
          <div class="row">
            <div class="col-md-6" id="master"></div>
            <div class="col-md-6" id="zookeeper"></div>
          </div>
        </div>
      </div>
      <div class="center-block">
        <div class="container">
          <div class="row">
            <div class="col-md-6">
              <br>
              <div class="plotHeading">Ingest (Entries/s)</div>
              <div id="ingest_entries" style="width:100%;height:200px;"></div>
            </div>
            <div class="col-md-6">
              <br>
              <div class="plotHeading">Scan (Entries/s)</div>
              <div id="scan_entries" style="width:100%;height:200px;"></div>
            </div>
          </div>
          <div class="row">
            <div class="col-md-6">
              <br>
              <div class="plotHeading">Ingest (MB/s)</div>
              <div id="ingest_mb" style="width:100%;height:200px;"></div>
            </div>
            <div class="col-md-6">
              <br>
              <div class="plotHeading">Scan (MB/s)</div>
              <div id="scan_mb" style="width:100%;height:200px;"></div>
            </div>
          </div>
          <div class="row">
            <div class="col-md-6">
              <br>
              <div class="plotHeading">Load Average</div>
              <div id="load_avg" style="width:100%;height:200px;"></div>
            </div>
            <div class="col-md-6">
              <br>
              <div class="plotHeading">Seeks</div>
              <div id="seeks" style="width:100%;height:200px;"></div>
            </div>
          </div>
          <div class="row">
            <div class="col-md-6">
              <br>
              <div class="plotHeading">Minor Compactions</div>
              <div id="minor" style="width:100%;height:200px;"></div>
            </div>
            <div class="col-md-6">
              <br>
              <div class="plotHeading">Major Compactions</div>
              <div id="major" style="width:100%;height:200px;"></div>
            </div>
          </div>
          <div class="row">
            <div class="col-md-6">
              <br>
              <div class="plotHeading">Index Cache Hit Rate</div>
              <div id="index_cache" style="width:100%;height:200px;"></div>
            </div>
            <div class="col-md-6">
              <br>
              <div class="plotHeading">Data Cache Hit Rate</div>
              <div id="data_cache" style="width:100%;height:200px;"></div>
            </div>
          </div>
        </div>
      </div>
