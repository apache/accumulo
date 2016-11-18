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
  		$.getJSON("rest/master", function(data) {
  			var items = [];
  			items.push("<td class='firstcell left'>" + data.master + "</td>");
  			items.push("<td class='right'>" + data.onlineTabletServers + "</td>");
  			items.push("<td class='right'>" + data.totalTabletServers + "</td>");
  			var date = new Date(parseInt(data.lastGC));
  			items.push("<td class='left'><a href='/gc'>" + date.toLocaleString() + "</a></td>");
  			items.push("<td class='right'>" + bigNumberForQuantity(data.tablets) + "</td>");
  			items.push("<td class='right'>" + bigNumberForQuantity(data.unassignedTablets) + "</td>");
  			items.push("<td class='right'>" + bigNumberForQuantity(data.numentries) + "</td>");
  			items.push("<td class='right'>" + bigNumberForQuantity(Math.round(data.ingestrate)) + "</td>");
  			items.push("<td class='right'>" + bigNumberForQuantity(Math.round(data.entriesRead)) + "</td>");
  			items.push("<td class='right'>" + bigNumberForQuantity(Math.round(data.queryrate)) + "</td>");
  			items.push("<td class='right'>" + timeDuration(data.holdTime) + "</td>");
  			items.push("<td class='right'>" + bigNumberForQuantity(data.osload) + "</td>");
  			
  			$("<tr/>", {
   			 html: items.join(""),
   			 class: "highlight"
  			}).appendTo("#masterStatus");
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
		    <a name='masterStatus'>&nbsp;</a>
			<table id='masterStatus' class='sortable'>
			  <caption>
				<span class='table-caption'>Master&nbsp;Status</span><br />
				<a href='/op?action=toggleLegend&redir=%2Fmaster&page=/master&table=masterStatus&show=true'>Show&nbsp;Legend</a>
			  </caption>
			  <tr><th class='firstcell'>Master</th><th>#&nbsp;Online<br />Tablet&nbsp;Servers</th><th>#&nbsp;Total<br />Tablet&nbsp;Servers</th><th>Last&nbsp;GC</th><th>#&nbsp;Tablets</th><th>#&nbsp;Unassigned<br />Tablets</th><th>Entries</th><th>Ingest</th><th>Entries<br />Read</th><th>Entries<br />Returned</th><th>Hold&nbsp;Time</th><th>OS&nbsp;Load</th></tr>
			</table>
		  </div>
		  
        </div>
      </div>    
    </div>
  </body>
</html>
