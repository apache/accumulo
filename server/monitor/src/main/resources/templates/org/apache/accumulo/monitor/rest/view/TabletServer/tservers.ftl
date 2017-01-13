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
  		$.getJSON("rest/tservers", function(data) {
  			var count = 0;
            
  			$.each(data.servers, function(key, val) {
              var items = [];
  			  items.push("<td class='firstcell left'><a href='/tservers/" + val.id + "'>" + val.hostname + "</a></td>");
  			  items.push("<td class='right'>" + bigNumberForQuantity(val.tablets) + "</td>");
			  items.push("<td class='right'>" + timeDuration(val.lastContact) + "</td>");
			  items.push("<td class='right'>" + bigNumberForQuantity(val.entries) + "</td>");
			  items.push("<td class='right'>" + bigNumberForQuantity(Math.floor(val.ingest)) + "</td>");
			  items.push("<td class='right'>" + bigNumberForQuantity(Math.floor(val.query)) + "</td>");
			  items.push("<td class='right'>" + timeDuration(val.holdtime) + "</td>");
			  items.push("<td class='right'>" + bigNumberForQuantity(val.compactions.scans.running) + "&nbsp;(" + val.compactions.scans.queued + ")</td>");
			  items.push("<td class='right'>" + bigNumberForQuantity(val.compactions.minor.running) + "&nbsp;(" + val.compactions.minor.queued + ")</td>");
			  items.push("<td class='right'>" + bigNumberForQuantity(val.compactions.major.running) + "&nbsp;(" + val.compactions.major.queued + ")</td>");
			  items.push("<td class='right'>" + Math.round(val.indexCacheHitRate*100) + "%</td>");
			  items.push("<td class='right'>" + Math.round(val.dataCacheHitRate*100) + "%</td>");
			  items.push("<td class='right'>" + bigNumberForQuantity(val.osload) + "</td>");
              
              if (count % 2 == 0) {
                $("<tr/>", {
                  html: items.join(""),
                  class: "highlight"
                }).appendTo("#tservers");
              } else {
                $("<tr/>", {
                  html: items.join("")
                }).appendTo("#tservers");
              }
              count += 1;
              
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
          <div>
			<a name='tservers'>&nbsp;</a>
			<table id='tservers' class='sortable'>
			  <caption>
				<span class='table-caption'>Tablet&nbsp;Servers</span><br />
				<span class='table-subcaption'>Click on the <span style='color: #0000ff;'>server address</span> to view detailed performance statistics for that server.</span><br />
				  <a href='/op?action=toggleLegend&redir=%2Ftservers&page=/tservers&table=tservers&show=true'>Show&nbsp;Legend</a>
			  </caption>
			  <tr><th class='firstcell'>Server</th><th>Hosted&nbsp;Tablets</th><th>Last&nbsp;Contact</th><th>Entries</th><th>Ingest</th><th>Query</th><th>Hold&nbsp;Time</th><th>Running<br />Scans</th><th>Minor<br />Compactions</th><th>Major<br />Compactions</th><th>Index Cache<br />Hit Rate</th><th>Data Cache<br />Hit Rate</th><th>OS&nbsp;Load</th></tr>
			</table>
		  </div>
        </div>
      </div>    
    </div>
  </body>
</html>
