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
  		$.getJSON("../../rest/trace/summary/${minutes}", function(data) {
            
            var count = 0;
            
            $.each(data.recentTraces, function(key, val) {
            
              var items = [];
              
              items.push("<td class='firstcell left'><a href='/trace/listType?type=" + val.type + "&minutes=${minutes}'>" + val.type + "</a></td>");
              items.push("<td class ='right'>" + bigNumberForQuantity(val.total) + "</td>");
              items.push("<td class='right'>" + timeDuration(val.min) + "</td>");
              items.push("<td class='right'>" + timeDuration(val.max) + "</td>");
              items.push("<td class='right'>" + timeDuration(val.avg) + "</td>");
              items.push("<td class='left'>");
              items.push("<table>");
              items.push("<tr>");
              
              $.each(val.histogram, function(key2, val2) {
              	items.push("<td style='width:5em'>" + (val2 == 0 ? "-" : val2) + "</td>");
              });
              items.push("</tr>");
              items.push("</table>");
              items.push("</td>");
              
              if (count % 2 == 0) {
              	$("<tr/>", {
                  html: items.join(""),
                  class: "highlight"
              	}).appendTo("#traceSummary"); 
              } else {
              	$("<tr/>", {
              	  html: items.join("")
              	}).appendTo("#traceSummary");
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
			<a name='traceSummary'>&nbsp;</a>
			<table id='traceSummary' class='sortable'>
			  <caption>
				<span class='table-caption'>All Traces</span><br />
				<a href='/op?action=toggleLegend&redir=%2Ftrace%2Fsummary%3Fminutes%3D10&page=/trace/summary&table=traceSummary&show=true'>Show&nbsp;Legend</a>
			  </caption>
			  <tr><th class='firstcell'>Type</th><th>Total</th><th>min</th><th>max</th><th>avg</th><th>Histogram</th></tr>
			</table>
		  </div>
          
        </div>
      </div>    
    </div>
  </body>
</html>
