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
  		$.getJSON("rest/scans", function(data) {
  			var items = [];
  			
  			$.each(data.scans, function(key, val) {
  			  items.push("<td class='firstcell left'><a href='/tservers/" + val.server + "'>" + val.server + "</a></td>");
  			  items.push("<td class='right'>" + val.scanCount + "</td>");
			  items.push("<td class='right'>" + timeDuration(val.oldestScan) + "</td>");
			});
  			
  			$("<tr/>", {
   			 html: items.join(""),
   			 class: "highlight"
  			}).appendTo("#scanStatus");
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
			<a name='scanStatus'>&nbsp;</a>
			<table id='scanStatus' class='sortable'>
			  <caption>
                <span class='table-caption'>Scan&nbsp;Status</span><br />
				<a href='/op?action=toggleLegend&redir=%2Fscans&page=/scans&table=scanStatus&show=true'>Show&nbsp;Legend</a>
			  </caption>
			<tr><th class='firstcell'>Server</th><th>#</th><th>Oldest&nbsp;Age</th></tr>
			</table>
		  </div>
        </div>
      </div>    
    </div>
  </body>
</html>
