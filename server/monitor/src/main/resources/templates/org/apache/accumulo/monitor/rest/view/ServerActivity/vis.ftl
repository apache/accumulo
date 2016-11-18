<!--
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
<html>
  <head>
    <title>${title} - Accumulo ${version}</title>
    <#if refresh gt 0 ><meta http-equiv='refresh' content='${refresh}' /></#if>
    <meta http-equiv='Content-Type' content='test/html"' />
    <meta http-equiv='Content-Script-Type' content='text/javascript' />
    <meta http-equiv='Content-Style-Type' content='text/css' />
    <link rel='shortcut icon' type='image/jpg' href='http://localhost:9995/web/favicon.png' />
    <link rel='stylesheet' type='text/css' href='http://localhost:9995/web/screen.css' media='screen' />
    <script src='http://localhost:9995/web/functions.js' type='text/javascript'></script>

    <!--[if lte IE 8]><script language="javascript" type="text/javascript" src="http://localhost:9995/web/flot/excanvas.min.js"></script><![endif]-->
    <script language="javascript" type="text/javascript" src="http://localhost:9995/web/flot/jquery.js"></script>
    <script language="javascript" type="text/javascript" src="http://localhost:9995/web/flot/jquery.flot.js"></script>
  </head>

  <body>
  	<script type="text/javascript">
  		$.getJSON("../rest/tservers/serverStats", function(data) {
  			
  			$.each(data.serverStats, function(key, val) {
              var item = val.description;
              
              $("<option/>", {
                  html: item,
                  class: "highlight"
              }).appendTo("#motion");
              
              $("<option/>", {
                  html: item,
                  class: "highlight"
              }).appendTo("#color");
              
			});
		});
		
  	</script> 
    <div id='content-wrapper'>
      <div id='content'>
        <div id='header'>
          <#include "/templates/header.ftl">
        </div>

        <#include "/templates/sidebar.ftl">

        <div id='main' style='bottom:0'>
          <div class='left'>
            <div id='parameters' class='nowrap'>
              <span class='viscontrol'>Shape: <select id='shape' onchange='setShape(this)'><option>Circles</option><option>Squares</option></select></span>
                &nbsp;&nbsp<span class='viscontrol'>Size: <select id='size' onchange='setSize(this)'><option>10</option><option>20</option><option selected='true'>40</option><option>80</option></select></span>
                &nbsp;&nbsp<span class='viscontrol'>Motion: <select id='motion' onchange='setMotion(this)'><option selected='true'></option></select></span>
                &nbsp;&nbsp<span class='viscontrol'>Color: <select id='color' onchange='setColor(this)'></select></span>
                &nbsp;&nbsp<span class='viscontrol'>(hover for info, click for details)</span></div>

            <div id='hoverable'>
              <div id='vishoverinfo'></div>

                <br><canvas id='visCanvas' width='640' height='640'>Browser does not support canvas.</canvas>

            </div>
          </div>
          <script type='text/javascript'>
            var numCores = 8;
            var jsonurl = 'http://localhost:50096/rest/json';
            var visurl = 'http://localhost:50096/vis';
            var serverurl = 'http://localhost:50096/rest/tservers/';

            // observable stats that can be connected to motion or color
            var statNames = {'osload': false,'ingest': false,'query': false,'ingestMB': false,'queryMB': false,'scans': false,'scansessions': false,'holdtime': false,'allavg': true,'allmax': true};
            var maxStatValues = {'osload': 8, 'ingest': 1000, 'query': 10000, 'ingestMB': 10, 'queryMB': 5, 'scans': 128, 'scansessions': 50, 'holdtime': 60000, 'allavg': 1, 'allmax': 1}; // initial values that are system-dependent may increase based on observed values
            var adjustMax = {'osload': true, 'ingest': true, 'query': true, 'ingestMB': true, 'queryMB': true, 'scans': false, 'scansessions': true, 'holdtime': false, 'allavg': false, 'allmax': false}; // whether to allow increases in the max based on observed values
            var significance = {'osload': 100.0, 'ingest': 1.0, 'query': 1.0, 'ingestMB': 10.0, 'queryMB': 10.0, 'scans': 1.0, 'scansessions': 10.0, 'holdtime': 1.0, 'allavg': 100.0, 'allmax': 100.0}; // values will be converted by floor(this*value)/this
            var numNormalStats = 8;
          </script>
          <script src='http://localhost:9995/web/vis.js' type='text/javascript'></script>

        </div>
      </div>    
    </div>
  </body>
</html>
