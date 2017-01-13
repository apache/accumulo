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
  		$.getJSON("../../rest/trace/listType/${type}/${minutes}", function(data) {
            
            var count = 0;          
            
            $.each(data.traces, function(key, val) {
            
              var items = [];
              
              var date = new Date(val.start);
              items.push("<td class='firstcell left'><a href='/trace/show?id=" + val.id + "'>" + date.toLocaleString() + "</a></td>");
              items.push("<td class ='right'>" + timeDuration(val.ms) + "</td>");
              items.push("<td class='left'>" + val.source + "</td>");
              
              if (count % 2 == 0) {
              	$("<tr/>", {
                  html: items.join(""),
                  class: "highlight"
              	}).appendTo("#trace"); 
              } else {
              	$("<tr/>", {
              	  html: items.join("")
              	}).appendTo("#trace");
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
			<a name='trace'>&nbsp;</a>
			<table id='trace' class='sortable'>
			  <caption>
				<span class='table-caption'>Traces for ${type}</span><br />
				<a href='/op?action=toggleLegend&redir=%2Ftrace%2FlistType%3Ftype%3DmajorCompaction%26minutes%3D10&page=/trace/listType&table=trace&show=true'>Show&nbsp;Legend</a>
			  </caption>
			  <tr><th class='firstcell'>Start</th><th>ms</th><th>Source</th></tr>
			</table>
		  </div>
          
        </div>
      </div>    
    </div>
  </body>
</html>
