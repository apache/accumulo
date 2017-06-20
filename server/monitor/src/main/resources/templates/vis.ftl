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
      <div>
        <div>
          <div id="parameters">
            <span class="viscontrol">Shape: <select id="shape" onchange="setShape(this)"><option>Circles</option><option>Squares</option></select></span>
            &nbsp;&nbsp;
            <span class="viscontrol">Size: <select id="size" onchange="setSize(this)"><option>10</option><option>20</option><option selected="true">40</option><option>80</option></select></span>
            &nbsp;&nbsp;
            <span class="viscontrol">Motion: <select id="motion" onchange="setMotion(this)"></select></span>
            &nbsp;&nbsp;
            <span class="viscontrol">Color: <select id="color" onchange="setColor(this)"></select></span>
          </div>
          <br>
          <span>Hover for info, click for details</span>
          <div id="hoverable">
            <div id="vishoverinfo"></div>
            <br>
            <canvas id="visCanvas" width="640" height="640">Browser does not support canvas.</canvas>
          </div>
        </div>
      </div>

      <script type="text/javascript">
        /**
         * Creates initial visualization table, passes the shape, size, motion, and color from the template
         */
        $(document).ready(function() {
          $.ajaxSetup({
            async: false
          });
          getServerStats();
          $.ajaxSetup({
            async: true
          });

          setStats();
          setOptions('${shape}', '${size}', '${motion}', '${color}');
          setState();

          drawGrid();
          getXML();
          refresh();
          drawDots();
        });
      </script>
      <script type="text/javascript">
        // Populates variables to be used in the visualization
        var numCores = 8;
        var jsonurl = '/rest/tservers';
        var visurl = '/vis';
        var serverurl = '/tservers?s=';

        var numNormalStats = 8;
      </script>

      <script src="/resources/js/vis.js"></script>
