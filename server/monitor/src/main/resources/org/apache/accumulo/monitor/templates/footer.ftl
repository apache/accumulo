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
    <footer id="footer">
      <div>
        <div class="input-group input-group-sm" style="float: left; width: 15%;">
          <span class="smalltext">Refresh&nbsp;Rate&nbsp;</span>
          <span class="input-group-btn">
            <button type="button" class="btn btn-default">
              <span class="glyphicon glyphicon-minus-sign"></span>
            </button>
          </span>
          <input type="text" class="form-control">
          <span class="input-group-btn">
            <button type="button" class="btn btn-default">
              <span class="glyphicon glyphicon-plus-sign">
            </button>
          </span>
        </div>
        <div style="float: left; width: 70%;">
          <div class="smalltext"><a href="https://accumulo.apache.org/" target="_blank">Apache&nbsp;Accumulo</a>&nbsp;
          ${version}
          </div>
          <div class="smalltext">${instance_id}</div>
          <div class="smalltext" id="currentDate"></div>
          <script>
            document.getElementById('currentDate').innerHTML = Date();
          </script>
        </div>
      </div>
    </footer>
