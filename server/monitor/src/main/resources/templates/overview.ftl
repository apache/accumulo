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
      <table class="noborder">
        <tr>
          <td class="noborder" id="master"></td>
          <td class="noborder" id="zookeeper"></td>
        </tr>
      </table>
      <br />
      <table class="noborder">          
        <tr>
          <td>
            <div class="plotHeading">Ingest (Entries/s)</div><br /><div id="ingest_entries" style="width:450px;height:200px;"></div>
          </td>
          <td>
            <div class="plotHeading">Scan (Entries/s)</div><br /><div id="scan_entries" style="width:450px;height:200px;"></div>
          </td>
        </tr>
        <tr>
          <td>
            <br /><div class="plotHeading">Ingest (MB/s)</div><br /><div id="ingest_mb" style="width:450px;height:200px;"></div>
          </td>
          <td>
            <br /><div class="plotHeading">Scan (MB/s)</div><br /><div id="scan_mb" style="width:450px;height:200px;"></div>
          </td>
        </tr>
        <tr>
          <td>
            <br /><div class="plotHeading">Load Average</div><br /><div id="load_avg" style="width:450px;height:200px;"></div>
          </td>
          <td>
            <br /><div class="plotHeading">Seeks</div><br /><div id="seeks" style="width:450px;height:200px;"></div>
          </td>
        </tr>
        <tr>
          <td>
            <br /><div class="plotHeading">Minor Compactions</div><br /><div id="minor" style="width:450px;height:200px;"></div>
          </td>
          <td>
            <br /><div class="plotHeading">Major Compactions</div><br /><div id="major" style="width:450px;height:200px;"></div>
          </td>
        </tr>
        <tr>
          <td>
            <br /><div class="plotHeading">Index Cache Hit Rate</div><br /><div id="index_cache" style="width:450px;height:200px;"></div>
          </td>
          <td>
            <br /><div class="plotHeading">Data Cache Hit Rate</div><br /><div id="data_cache" style="width:450px;height:200px;"></div>
          </td>
        </tr>
      </table> 
