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
  		$.getJSON("rest/gc", function(data) {
            
            var count = 0;
            
            if (data.files.lastCycle.finished > 0) {
                var items = [];
                
                var working = data.files.lastCycle;
            
                items.push("<td class='firstcell left'>File&nbsp;Collection,&nbsp;Last&nbsp;Cycle</td>");
                var date = new Date(working.finished);
                items.push("<td class='right'>" + date.toLocaleString() + "</td>");
                items.push("<td class='right'>" + bigNumberForQuantity(working.candidates) + "</td>");
                items.push("<td class='right'>" + bigNumberForQuantity(working.deleted) + "</td>");
                items.push("<td class='right'>" + bigNumberForQuantity(working.inUse) + "</td>");
                items.push("<td class='right'>" + bigNumberForQuantity(working.errors) + "</td>");                
                items.push("<td class='right'>" + timeDuration(working.finished - working.started) + "</td>");
                
                if (count % 2 == 0) {
                  $("<tr/>", {
                    html: items.join(""),
                    class: "highlight"
                  }).appendTo("#gcActivity");   
                } else {
                  $("<tr/>", {
                    html: items.join("")
                  }).appendTo("#gcActivity");   
                }
                count += 1;
            }
            
            if (data.files.currentCycle.started > 0) {               
                var items = [];
                
                var working = data.files.currentCycle;
            
                items.push("<td class='firstcell left'>File&nbsp;Collection,&nbsp;Running</td>");
                var date = new Date(working.finished);
                items.push("<td class='right'>" + date.toLocaleString() + "</td>");
                items.push("<td class='right'>" + bigNumberForQuantity(working.candidates) + "</td>");
                items.push("<td class='right'>" + bigNumberForQuantity(working.deleted) + "</td>");
                items.push("<td class='right'>" + bigNumberForQuantity(working.inUse) + "</td>");
                items.push("<td class='right'>" + bigNumberForQuantity(working.errors) + "</td>");                
                items.push("<td class='right'>" + timeDuration(working.finished - working.started) + "</td>");
                
                if (count % 2 == 0) {
                  $("<tr/>", {
                    html: items.join(""),
                    class: "highlight"
                  }).appendTo("#gcActivity");   
                } else {
                  $("<tr/>", {
                    html: items.join("")
                  }).appendTo("#gcActivity");   
                }
                count += 1;
            }
            if (data.wals.lastCycle.finished > 0) {
                var items = [];
                
                var working = data.wals.lastCycle;
            
                items.push("<td class='firstcell left'>WAL&nbsp;Collection,&nbsp;Last&nbsp;Cycle</td>");
                var date = new Date(working.finished);
                items.push("<td class='right'>" + date.toLocaleString() + "</td>");
                items.push("<td class='right'>" + bigNumberForQuantity(working.candidates) + "</td>");
                items.push("<td class='right'>" + bigNumberForQuantity(working.deleted) + "</td>");
                items.push("<td class='right'>" + bigNumberForQuantity(working.inUse) + "</td>");
                items.push("<td class='right'>" + bigNumberForQuantity(working.errors) + "</td>");                
                items.push("<td class='right'>" + timeDuration(working.finished - working.started) + "</td>");
                
                if (count % 2 == 0) {
                  $("<tr/>", {
                    html: items.join(""),
                    class: "highlight"
                  }).appendTo("#gcActivity");   
                } else {
                  $("<tr/>", {
                    html: items.join("")
                  }).appendTo("#gcActivity");   
                }
                count += 1;
            }
            if (data.wals.currentCycle.started > 0) {
                var items = [];
                
                var working = data.wals.currentCycle;
            
                items.push("<td class='firstcell left'>WAL&nbsp;Collection,&nbsp;Running</td>");
                var date = new Date(working.finished);
                items.push("<td class='right'>" + date.toLocaleString() + "</td>");
                items.push("<td class='right'>" + bigNumberForQuantity(working.candidates) + "</td>");
                items.push("<td class='right'>" + bigNumberForQuantity(working.deleted) + "</td>");
                items.push("<td class='right'>" + bigNumberForQuantity(working.inUse) + "</td>");
                items.push("<td class='right'>" + bigNumberForQuantity(working.errors) + "</td>");                
                items.push("<td class='right'>" + timeDuration(working.finished - working.started) + "</td>");
                
                if (count % 2 == 0) {
                  $("<tr/>", {
                    html: items.join(""),
                    class: "highlight"
                  }).appendTo("#gcActivity");   
                } else {
                  $("<tr/>", {
                    html: items.join("")
                  }).appendTo("#gcActivity");   
                }
                count += 1;
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
            <a name='gcActivity'>&nbsp;</a>
            <table id='gcActivity' class='sortable'>
              <caption>
                <span class='table-caption'>Collection&nbsp;Activity</span><br />
              </caption>
              <tr><th class='firstcell'><a href='/op?action=sortTable&redir=%2Fgc&page=/gc&table=gcActivity&asc=false'>Activity&nbsp;<img width='10px' height='10px' src='/web/up.gif' alt='v' /></a></th><th><a href='/op?action=sortTable&redir=%2Fgc&page=/gc&table=gcActivity&col=1'>Finished</a></th><th><a href='/op?action=sortTable&redir=%2Fgc&page=/gc&table=gcActivity&col=2'>Candidates</a></th><th><a href='/op?action=sortTable&redir=%2Fgc&page=/gc&table=gcActivity&col=3'>Deleted</a></th><th><a href='/op?action=sortTable&redir=%2Fgc&page=/gc&table=gcActivity&col=4'>In&nbsp;Use</a></th><th><a href='/op?action=sortTable&redir=%2Fgc&page=/gc&table=gcActivity&col=5'>Errors</a></th><th><a href='/op?action=sortTable&redir=%2Fgc&page=/gc&table=gcActivity&col=6'>Duration</a></th></tr>
            </table>
          </div>
        </div>
      </div>    
    </div>
  </body>
</html>
