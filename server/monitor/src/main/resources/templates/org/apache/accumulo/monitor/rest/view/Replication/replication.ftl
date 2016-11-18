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
  		$.getJSON("../rest/tables/${tableID}", function(data) {
            var count = 0;
  			
  			$.each(data.servers, function(key, val) {
              var items = [];
  			  items.push("<td class='firstcell left'><a href='/tables/" + val.tableId + "'>" + val.tableName + "</a></td>");
  			  items.push("<td class='right'>" + val.peerName + "</td>");
			  items.push("<td class='right'>" + val.remoteIdentifier + "</td>");
			  items.push("<td class='right'>" + val.replicaSystemType + "</td>");
			  items.push("<td class='right'>" + bigNumberForQuantity(val.filesNeedingReplication) + "</td>");
              
              if (count % 2 == 0) {
                $("<tr/>", {
                  html: items.join(""),
                  class: "highlight"
                }).appendTo("#participatingTServers");
              } else {
                $("<tr/>", {
                  html: items.join("")
                }).appendTo("#participatingTServers");
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
            <a name='participatingTServers'>&nbsp;</a>
            <table id='participatingTServers' class='sortable'>
              <caption>
                <span class='table-caption'>Replication Status</span><br />
                <span class='table-subcaption'></span><br />
                <a href='/op?action=toggleLegend&redir=%2Ftables%3Ft%3D1&page=/tables&table=participatingTServers&show=true'>Show&nbsp;Legend</a>
              </caption>
              <tr><th class='firstcell'>Table</th><th>Peer</th><th>remote&nbsp;Identifier</th><th>Replica System Type</th><th>Files needing replication</th></tr>
            </table>
          </div>
        </div>
      </div>    
    </div>
  </body>
</html>
