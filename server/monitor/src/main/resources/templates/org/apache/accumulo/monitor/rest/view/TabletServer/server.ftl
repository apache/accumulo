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
  		$.getJSON("../rest/tservers/${server}", function(data) {
  			var items = [];
  			
  			items.push("<td class='firstcell right'>" + bigNumberForQuantity(data.details.hostedTablets) + "</td>");
            items.push("<td class='right'>" + bigNumberForQuantity(data.details.entries) + "</td>");
  			items.push("<td class='right'>" + bigNumberForQuantity(data.details.minors) + "</td>");
			items.push("<td class='right'>" + bigNumberForQuantity(data.details.majors) + "</td>");
			items.push("<td class='right'>" + bigNumberForQuantity(data.details.splits) + "</td>");
  			
  			$("<tr/>", {
   			 html: items.join(""),
   			 class: "highlight"
  			}).appendTo("#tServerDetail");
            
            var totalTimeSpent = 0;
            $.each(data.allTimeTabletResults, function(key, val) {
              totalTimeSpent += val.timeSpent;
            })
            
            var count = 0;
            $.each(data.allTimeTabletResults, function(key, val) {
              var row = [];
              
              row.push("<td class='firstcell left'>" + val.operation + "</td>");
              row.push("<td class='right'>" + bigNumberForQuantity(val.success) + "</td>");
              row.push("<td class='right'>" + bigNumberForQuantity(val.failure) + "</td>");
              row.push("<td class='right'>" + (val.avgQueueTime == null ? "&mdash;" : timeDuration(val.avgQueueTime * 1000.0)) + "</td>");
              row.push("<td class='right'>" + (val.queueStdDev == null ? "&mdash;" : timeDuration(val.queueStdDev * 1000.0)) + "</td>");
              row.push("<td class='right'>" + (val.avgTime == null ? "&mdash;" : timeDuration(val.avgTime * 1000.0)) + "</td>");
              row.push("<td class='right'>" + (val.stdDev == null ? "&mdash;" : timeDuration(val.stdDev * 1000.0)) + "</td>");
              row.push("<td class='right'><div class='progress-chart'><div style='width:" + Math.floor((val.timeSpent / totalTimeSpent) * 100) + "%;'></div></div>&nbsp;" + Math.floor((val.timeSpent / totalTimeSpent) * 100) + "%</td>");
              
              if (count % 2 == 0) {
                  $("<tr/>", {
                      html: row.join(""),
                      class: "highlight"
                  }).appendTo("#opHistoryDetails");
              } else {
                  $("<tr/>", {
                      html: row.join("")
                  }).appendTo("#opHistoryDetails");
              }
              count += 1;
            });
            
            var items = [];
            var current = data.currentTabletOperationResults;
            
            items.push("<td class='firstcell right'>" + (current.currentMinorAvg == null ? "&mdash;" : timeDuration(current.currentMinorAvg * 1000.0)) + "</td>")
            items.push("<td class='right'>" + (current.currentMinorStdDev == null ? "&mdash;" : timeDuration(current.currentMinorStdDev * 1000.0)) + "</td>")
            items.push("<td class='right'>" + (current.currentMajorAvg == null ? "&mdash;" : timeDuration(current.currentMajorAvg * 1000.0)) + "</td>")
            items.push("<td class='right'>" + (current.currentMajorStdDev == null ? "&mdash;" : timeDuration(current.currentMajorStdDev * 1000.0)) + "</td>")
            
            $("<tr/>", {
                html: items.join(""),
                class: "highlight"
            }).appendTo("#currentTabletOps");
            
            
            var count = 0;
            $.each(data.currentOperations, function(key, val) {
              var row = [];
              
              row.push("<td class='firstcell left'><a href='/tables/" + val.tableID + "'>" + val.name + "</a></td>");
              row.push("<td class='left'><code>" + val.tablet + "</code></td>");
              row.push("<td class='right'>" + bigNumberForQuantity(val.entries) + "</td>");
              row.push("<td class='right'>" + bigNumberForQuantity(Math.floor(val.ingest)) + "</td>");
              row.push("<td class='right'>" + bigNumberForQuantity(Math.floor(val.query)) + "</td>");
              row.push("<td class='right'>" + (val.minorAvg == null ? "&mdash;" : timeDuration(val.minorAvg * 1000.0)) + "</td>");
              row.push("<td class='right'>" + (val.minorStdDev == null ? "&mdash;" : timeDuration(val.minorStdDev * 1000.0)) + "</td>");
              row.push("<td class='right'>" + bigNumberForQuantity(Math.floor(val.minorAvgES)) + "</td>");
              row.push("<td class='right'>" + (val.majorAvg == null ? "&mdash;" : timeDuration(val.majorAvg * 1000.0)) + "</td>");
              row.push("<td class='right'>" + (val.majorStdDev == null ? "&mdash;" : timeDuration(val.majorStdDev * 1000.0)) + "</td>");
              row.push("<td class='right'>" + bigNumberForQuantity(Math.floor(val.majorAvgES)) + "</td>");
              
              if (count % 2 == 0) {
                  $("<tr/>", {
                      html: row.join(""),
                      class: "highlight"
                  }).appendTo("#perTabletResults");
              } else {
                  $("<tr/>", {
                      html: row.join("")
                  }).appendTo("#perTabletResults");
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
            <a name='tServerDetail'>&nbsp;</a>
            <table id='tServerDetail' class='sortable'>
              <caption>
                <span class='table-caption'>Details</span><br />
                <span class='table-subcaption'>${server}</span><br />
              </caption>
             <tr><th class='firstcell'>Hosted&nbsp;Tablets</th><th>Entries</th><th>Minor&nbsp;Compacting</th><th>Major&nbsp;Compacting</th><th>Splitting</th></tr>
            </table>
          </div>

          <div>
            <a name='opHistoryDetails'>&nbsp;</a>
            <table id='opHistoryDetails' class='sortable'>
              <caption>
                <span class='table-caption'>All-Time&nbsp;Tablet&nbsp;Operation&nbsp;Results</span><br />
              </caption>
              <tr><th class='firstcell'><a href='/op?action=sortTable&redir=%2Ftservers%3Fs%3Dlocalhost%3A9997&page=/tservers&table=opHistoryDetails&asc=false'>Operation&nbsp;<img width='10px' height='10px' src='/web/up.gif' alt='v' /></a></th><th><a href='/op?action=sortTable&redir=%2Ftservers%3Fs%3Dlocalhost%3A9997&page=/tservers&table=opHistoryDetails&col=1'>Success</a></th><th><a href='/op?action=sortTable&redir=%2Ftservers%3Fs%3Dlocalhost%3A9997&page=/tservers&table=opHistoryDetails&col=2'>Failure</a></th><th><a href='/op?action=sortTable&redir=%2Ftservers%3Fs%3Dlocalhost%3A9997&page=/tservers&table=opHistoryDetails&col=3'>Average<br />Queue&nbsp;Time</a></th><th><a href='/op?action=sortTable&redir=%2Ftservers%3Fs%3Dlocalhost%3A9997&page=/tservers&table=opHistoryDetails&col=4'>Std.&nbsp;Dev.<br />Queue&nbsp;Time</a></th><th><a href='/op?action=sortTable&redir=%2Ftservers%3Fs%3Dlocalhost%3A9997&page=/tservers&table=opHistoryDetails&col=5'>Average<br />Time</a></th><th><a href='/op?action=sortTable&redir=%2Ftservers%3Fs%3Dlocalhost%3A9997&page=/tservers&table=opHistoryDetails&col=6'>Std.&nbsp;Dev.<br />Time</a></th><th><a href='/op?action=sortTable&redir=%2Ftservers%3Fs%3Dlocalhost%3A9997&page=/tservers&table=opHistoryDetails&col=7'>Percentage&nbsp;Time&nbsp;Spent</a></th></tr>
            </table>
          </div>

          <div>
            <a name='currentTabletOps'>&nbsp;</a>
            <table id='currentTabletOps' class='sortable'>
              <caption>
                <span class='table-caption'>Current&nbsp;Tablet&nbsp;Operation&nbsp;Results</span><br />
              </caption>
              <tr><th class='firstcell'>Minor&nbsp;Average</th><th>Minor&nbsp;Std&nbsp;Dev</th><th>Major&nbsp;Avg</th><th>Major&nbsp;Std&nbsp;Dev</th></tr>
            </table>
          </div>

          <div>
            <a name='perTabletResults'>&nbsp;</a>
            <table id='perTabletResults' class='sortable'>
              <caption>
                <span class='table-caption'>Detailed&nbsp;Current&nbsp;Operations</span><br />
                <span class='table-subcaption'>Per-tablet&nbsp;Details</span><br />
              </caption>
              <tr><th class='firstcell'><a href='/op?action=sortTable&redir=%2Ftservers%3Fs%3Dlocalhost%3A9997&page=/tservers&table=perTabletResults&asc=false'>Table&nbsp;<img width='10px' height='10px' src='/web/up.gif' alt='v' /></a></th><th><a href='/op?action=sortTable&redir=%2Ftservers%3Fs%3Dlocalhost%3A9997&page=/tservers&table=perTabletResults&col=1'>Tablet</a></th><th><a href='/op?action=sortTable&redir=%2Ftservers%3Fs%3Dlocalhost%3A9997&page=/tservers&table=perTabletResults&col=2'>Entries</a></th><th><a href='/op?action=sortTable&redir=%2Ftservers%3Fs%3Dlocalhost%3A9997&page=/tservers&table=perTabletResults&col=3'>Ingest</a></th><th><a href='/op?action=sortTable&redir=%2Ftservers%3Fs%3Dlocalhost%3A9997&page=/tservers&table=perTabletResults&col=4'>Query</a></th><th><a href='/op?action=sortTable&redir=%2Ftservers%3Fs%3Dlocalhost%3A9997&page=/tservers&table=perTabletResults&col=5'>Minor&nbsp;Avg</a></th><th><a href='/op?action=sortTable&redir=%2Ftservers%3Fs%3Dlocalhost%3A9997&page=/tservers&table=perTabletResults&col=6'>Minor&nbsp;Std&nbsp;Dev</a></th><th><a href='/op?action=sortTable&redir=%2Ftservers%3Fs%3Dlocalhost%3A9997&page=/tservers&table=perTabletResults&col=7'>Minor&nbsp;Avg&nbsp;e/s</a></th><th><a href='/op?action=sortTable&redir=%2Ftservers%3Fs%3Dlocalhost%3A9997&page=/tservers&table=perTabletResults&col=8'>Major&nbsp;Avg</a></th><th><a href='/op?action=sortTable&redir=%2Ftservers%3Fs%3Dlocalhost%3A9997&page=/tservers&table=perTabletResults&col=9'>Major&nbsp;Std&nbsp;Dev</a></th><th><a href='/op?action=sortTable&redir=%2Ftservers%3Fs%3Dlocalhost%3A9997&page=/tservers&table=perTabletResults&col=10'>Major&nbsp;Avg&nbsp;e/s</a></th></tr>
            </table>
          </div>
          
        </div>
      </div>    
    </div>
  </body>
</html>
