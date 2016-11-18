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
  		$.getJSON("master", function(data) {
  			var items = [];
  			items.push("<tr><th colspan='2'><a href='/master'>Accumulo&nbsp;Master</a></th></tr>");
  			items.push("<tr class='highlight'><td class='left'><a href='/tables'>Tables</a></td><td class='right'>" + data.tables + "</td></tr>");
  			items.push("<tr><td class='left'><a href='/tservers'>Tablet&nbsp;Servers</a></td><td class='right'>" + data.totalTabletServers + "</td></tr>");
  			items.push("<tr class='highlight'><td class='left'><a href='/tservers'>Dead&nbsp;Tablet&nbsp;Servers</a></td><td class='right'>" + data.deadTabletServersCount + "</td></tr>");
  			items.push("<tr><td class='left'>Tablets</td><td class='right'>" + data.tablets + "</td></tr>");
  			items.push("<tr class='highlight'><td class='left'>Entries</td><td class='right'>" + data.numentries + "</td></tr>");
  			items.push("<tr><td class='left'>Lookups</td><td class='right'>" + data.lookups + "</td></tr>");
  			items.push("<tr class='highlight'><td class='left'>Uptime</td><td class='right'>" + data.uptime + "</td></tr>");
 
  			$("<table/>", {
   			 html: items.join("")
  			}).appendTo("#master");
		});
		
		$.getJSON("zk", function(data) {
			var items = [];
			items.push("<tr><th colspan='3'>Zookeeper</th></tr>");
			items.push("<tr><th>Server</th><th>Mode</th><th>Clients</th></tr>");
			$.each(data.zkServers, function(key, val) {
				items.push("<tr class='highlight'><td class='left'>" + val.server + "</td>");
				items.push("<td class='left'>" + val.mode + "</td>");
				items.push("<td class='right'>" + val.clients + "</td></tr>");
			});
			
  			$("<table/>", {
   			 html: items.join("")
  			}).appendTo("#zookeeper");
		});
		
  	</script>  	
    <div id='content-wrapper'>
      <div id='content'>
        <div id='header'>
          <div id='headertitle'>
            <h1>${title}</h1>
          </div>
          <div id='subheader'>Instance&nbsp;Name:&nbsp;${instance_name}&nbsp;&nbsp;&nbsp;Version:&nbsp;${version}
            <br><span class='smalltext'>Instance&nbsp;ID:&nbsp;${instance_id}</span>
            <br><span class='smalltext'>${current_date}</span>
          </div>
        </div>

        <#include "/templates/sidebar.ftl">

        <div id='main' style='bottom:0'>
          <table class='noborder'>
        	<tr>
        	  <td class='noborder' id='master'></td>
        	  <td class='noborder' id='zookeeper'></td>
        	</tr>
       	  </table>
        </div>
      </div>    
    </div>
  </body>
</html>