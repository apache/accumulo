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
  		$.getJSON("rest/bulkImports", function(data) {
  			var items = [];
  			
  			if (data.bulkImport.length === 0) {
  			  items.push("<td class='center' colspan='3'><i>Empty</i></td>");
  			  
  			  $("<tr/>", {
   			 	html: items.join("")
  			  }).appendTo("#masterBulkImportStatus");
  			  
  			} else {
  			  $.each(data.bulkImport, function(key, val) {
  			  	items.push("<td class='firstcell left'>" + val.filename + "</td>");
  			  	items.push("<td class='right'>" + val.age + "</td>");
  			  	items.push("<td class='right'>" + val.state + "</td>");
  			  });
  			  
  			  $("<tr/>", {
   			 	html: items.join(""),
   			 	class: "highlight"
  			  }).appendTo("#masterBulkImportStatus");
  			}
 
 			var items2 = [];
            if (data.tabletServerBulkImport.length === 0) {
                items2.push("<td class='center' colspan='3'><i>Empty</i></td>");
                $("<tr/>", {
                    html: items2.join("")
                }).appendTo("#bulkImportStatus");
            } else {
                $.each(data.tabletServerBulkImport, function(key, val) {
                  items2.push("<td class='firstcell left'><a href='/tservers/" + val.server + "'>" + val.server + "</a></td>");
                  items2.push("<td class='right'>" + val.importSize + "</td>");
                  items2.push("<td class='right'>" + (val.oldestAge > 0 ? val.oldestAge : "&mdash;") + "</td>");
                });
                
                $("<tr/>", {
                 html: items2.join(""),
                 class: "highlight"
                }).appendTo("#bulkImportStatus");
            }
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
			<a name='masterBulkImportStatus'>&nbsp;</a>
			<table id='masterBulkImportStatus' class='sortable'>
			  <caption>
				<span class='table-caption'>Bulk&nbsp;Import&nbsp;Status</span><br />
				<a href='/op?action=toggleLegend&redir=%2FbulkImports&page=/bulkImports&table=masterBulkImportStatus&show=true'>Show&nbsp;Legend</a>
			  </caption>
			  <tr><th class='firstcell'>Directory</th><th>Age</th><th>State</th></tr>
			</table>
		  </div>

		  <div>
			<a name='bulkImportStatus'>&nbsp;</a>
			<table id='bulkImportStatus' class='sortable'>
			  <caption>
				<span class='table-caption'>TabletServer&nbsp;Bulk&nbsp;Import&nbsp;Status</span><br />
				<a href='/op?action=toggleLegend&redir=%2FbulkImports&page=/bulkImports&table=bulkImportStatus&show=true'>Show&nbsp;Legend</a>
			  </caption>
			  <tr><th class='firstcell'>Server</th><th>#</th><th>Oldest&nbsp;Age</th></tr>
			</table>
		  </div>
        </div>
      </div>    
    </div>
  </body>
</html>
