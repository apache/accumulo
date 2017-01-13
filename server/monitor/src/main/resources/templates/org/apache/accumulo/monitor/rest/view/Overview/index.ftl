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
  			items.push("<tr><th colspan='2'><a href='/master'>Accumulo&nbsp;Master</a></th></tr>");
  			items.push("<tr class='highlight'><td class='left'><a href='/tables'>Tables</a></td><td class='right'>" + bigNumberForQuantity(data.tables) + "</td></tr>");
  			items.push("<tr><td class='left'><a href='/tservers'>Tablet&nbsp;Servers</a></td><td class='right'>" + bigNumberForQuantity(data.totalTabletServers) + "</td></tr>");
  			items.push("<tr class='highlight'><td class='left'><a href='/tservers'>Dead&nbsp;Tablet&nbsp;Servers</a></td><td class='right'>" + bigNumberForQuantity(data.deadTabletServersCount) + "</td></tr>");
  			items.push("<tr><td class='left'>Tablets</td><td class='right'>" + bigNumberForQuantity(data.tablets) + "</td></tr>");
  			items.push("<tr class='highlight'><td class='left'>Entries</td><td class='right'>" + bigNumberForQuantity(data.numentries) + "</td></tr>");
  			items.push("<tr><td class='left'>Lookups</td><td class='right'>" + bigNumberForQuantity(data.lookups) + "</td></tr>");
  			items.push("<tr class='highlight'><td class='left'>Uptime</td><td class='right'>" + timeDuration(data.uptime) + "</td></tr>");
 
  			$("<table/>", {
   			 html: items.join("")
  			}).appendTo("#master");
		});
		
		$.getJSON("rest/zk", function(data) {
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
        
        $.getJSON("rest/statistics/time/ingestRate", function(data) {
           var d0 = [];
           $.each(data, function(key, val) {
               d0.push([val.first, val.second]);
           });
           $.plot($("#ingest_entries"),[{ data: d0, lines: { show: true }, color:"red" }], {yaxis:{}, xaxis:{mode:"time",minTickSize: [1, "minute"],timeformat: "%H:%M<br />EST", ticks:3}});
        });
        
        $.getJSON("rest/statistics/time/scanEntries", function(data) {
           var d0 = [];
           var d1 = [];
           $.each(data, function(key, val) {
               if (val.first == "Read") {
                   $.each(val.second, function(key2, val2) {
                       d0.push([val2.first, val2.second]);
                   });
               }
               if (val.first == "Returned") {
                   $.each(val.second, function(key2, val2) {
                       d1.push([val2.first, val2.second]);
                   })
               }
           });
           
           $.plot($("#scan_entries"),[{ label: "Read", data: d0, lines: { show: true }, color:"red" },{ label: "Returned", data: d1, lines: { show: true }, color:"blue" }], {yaxis:{}, xaxis:{mode:"time",minTickSize: [1, "minute"],timeformat: "%H:%M<br />EST", ticks:3}});
        });
        
        $.getJSON("rest/statistics/time/ingestByteRate", function(data) {
           var d0 = [];
           $.each(data, function(key, val) {
               d0.push([val.first, val.second]);
           });
           $.plot($("#ingest_mb"),[{ data: d0, lines: { show: true }, color:"red" }], {yaxis:{}, xaxis:{mode:"time",minTickSize: [1, "minute"],timeformat: "%H:%M<br />EST", ticks:3}});
        });
        
        $.getJSON("rest/statistics/time/queryByteRate", function(data) {
           var d0 = [];
           $.each(data, function(key, val) {
               d0.push([val.first, val.second]);
           });
           $.plot($("#scan_mb"),[{ data: d0, lines: { show: true }, color:"red" }], {yaxis:{}, xaxis:{mode:"time",minTickSize: [1, "minute"],timeformat: "%H:%M<br />EST", ticks:3}});
        });
        
        $.getJSON("rest/statistics/time/load", function(data) {
           var d0 = [];
           $.each(data, function(key, val) {
               d0.push([val.first, val.second]);
           });
           $.plot($("#load_avg"),[{ data: d0, lines: { show: true }, color:"red" }], {yaxis:{}, xaxis:{mode:"time",minTickSize: [1, "minute"],timeformat: "%H:%M<br />EST", ticks:3}});
        });
        
        $.getJSON("rest/statistics/time/lookups", function(data) {
           var d0 = [];
           $.each(data, function(key, val) {
               d0.push([val.first, val.second]);
           });
           $.plot($("#seeks"),[{ data: d0, lines: { show: true }, color:"red" }], {yaxis:{}, xaxis:{mode:"time",minTickSize: [1, "minute"],timeformat: "%H:%M<br />EST", ticks:3}});
        });
        
        $.getJSON("rest/statistics/time/minorCompactions", function(data) {
           var d0 = [];
           $.each(data, function(key, val) {
               d0.push([val.first, val.second]);
           });
           $.plot($("#minor"),[{ data: d0, lines: { show: true }, color:"red" }], {yaxis:{}, xaxis:{mode:"time",minTickSize: [1, "minute"],timeformat: "%H:%M<br />EST", ticks:3}});
        });
        
        $.getJSON("rest/statistics/time/majorCompactions", function(data) {
           var d0 = [];
           $.each(data, function(key, val) {
               d0.push([val.first, val.second]);
           });
           $.plot($("#major"),[{ data: d0, lines: { show: true }, color:"red" }], {yaxis:{}, xaxis:{mode:"time",minTickSize: [1, "minute"],timeformat: "%H:%M<br />EST", ticks:3}});
        });
        
        $.getJSON("rest/statistics/time/indexCacheHitRate", function(data) {
           var d0 = [];
           $.each(data, function(key, val) {
               d0.push([val.first, val.second]);
           });
           $.plot($("#index_cache"),[{ data: d0, points: { show: true, radius: 1 }, color:"red" }], {yaxis:{}, xaxis:{mode:"time",minTickSize: [1, "minute"],timeformat: "%H:%M<br />EST", ticks:3}});
        });
        
        $.getJSON("rest/statistics/time/dataCacheHitRate", function(data) {
           var d0 = [];
           $.each(data, function(key, val) {
               d0.push([val.first, val.second]);
           });
           $.plot($("#data_cache"),[{ data: d0, points: { show: true, radius: 1 }, color:"red" }], {yaxis:{}, xaxis:{mode:"time",minTickSize: [1, "minute"],timeformat: "%H:%M<br />EST", ticks:3}});
        });
		
  	</script>  	
    <div id='content-wrapper'>
      <div id='content'>
        <div id='header'>
          <#include "/templates/header.ftl">
        </div>

        <#include "/templates/sidebar.ftl">

        <div id='main' style='bottom:0'>
            
          <table class='noborder'>
        	<tr>
        	  <td class='noborder' id='master'></td>
        	  <td class='noborder' id='zookeeper'></td>
        	</tr>
       	  </table>
          <br />
          <table class="noborder">          
            <tr>
              <td>
                <div class="plotHeading">Ingest (Entries/s)</div></br><div id="ingest_entries" style="width:450px;height:150px;"></div>
              </td>
              <td>
                <div class="plotHeading">Scan (Entries/s)</div></br><div id="scan_entries" style="width:450px;height:150px;"></div>
              </td>
            </tr>
            <tr>
              <td>
                <div class="plotHeading">Ingest (MB/s)</div></br><div id="ingest_mb" style="width:450px;height:150px;"></div>
              </td>
              <td>
                <div class="plotHeading">Scan (MB/s)</div></br><div id="scan_mb" style="width:450px;height:150px;"></div>
              </td>
            </tr>
            <tr>
              <td>
                <div class="plotHeading">Load Average</div></br><div id="load_avg" style="width:450px;height:150px;"></div>
              </td>
              <td>
                <div class="plotHeading">Seeks</div></br><div id="seeks" style="width:450px;height:150px;"></div>
              </td>
            </tr>
            <tr>
              <td>
                <div class="plotHeading">Minor Compactions</div></br><div id="minor" style="width:450px;height:150px;"></div>
              </td>
              <td>
                <div class="plotHeading">Major Compactions</div></br><div id="major" style="width:450px;height:150px;"></div>
              </td>
            </tr>
            <tr>
              <td>
                <div class="plotHeading">Index Cache Hit Rate</div></br><div id="index_cache" style="width:450px;height:150px;"></div>
              </td>
              <td>
                <div class="plotHeading">Data Cache Hit Rate</div></br><div id="data_cache" style="width:450px;height:150px;"></div>
              </td>
            </tr>
          </table>         
          
        </div>
      </div>    
    </div>
  </body>
</html>
