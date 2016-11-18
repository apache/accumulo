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
  	<style>
 	  td.right { text-align: right }
	  table.indent { position: relative; left: 10% }
 	  td.left { text-align: left }
	</style>
	
  	<script type="text/javascript">
  	  function toggle(id) {
 		var elt = document.getElementById(id);
 		if (elt.style.display=='none') {
    	  elt.style.display='table-row';
 		} else { 
    	  elt.style.display='none';
  		}
	  }
	  function pageload() {
  		var checkboxes = document.getElementsByTagName('input');
  		for (var i = 0; i < checkboxes.length; i++) {
    	  if (checkboxes[i].checked) {
      		var idSuffixOffset = checkboxes[i].id.indexOf('_checkbox');
      		var id = checkboxes[i].id.substring(0, idSuffixOffset);
      		document.getElementById(id).style.display='table-row';
    	  }
  		}
	  }
	  
  	  $.getJSON("../../rest/trace/show/${id}", function(data) {
        var id = 101010101;
        var date = new Date(data.start);
        $("#caption").append(date.toLocaleString());      
            
        $.each(data.traces, function(key, val) {
            
          var items = [];
          
          items.push("<tr>");
          items.push("<td class='right'>" + val.time + "+</td>");
          items.push("<td class='left'>" + val.start + "</td>");
          items.push("<td style='text-indent: " + val.level + "0px'>" + val.location + "</td>");
          items.push("<td>" + val.name + "</td>");
            
          if (val.addlData.data.length !== 0 || val.addlData.annotations.length !== 0) {
              
            items.push("<td><input type='checkbox' id=\"" + id + "_checkbox\" onclick='toggle(\"" + id + "\")'></td>");
            items.push("</tr>");
            items.push("<tr id='" + id + "' style='display:none'>");
            items.push("<td colspan='5'>");
            items.push("<table class='indent,noborder'>");
              	
            if (val.addlData.data.length !== 0) {
              items.push("<tr><th>Key</th><th>Value</th></tr>");
            	
              $.each(val.addlData.data, function(key2, val2) {
                items.push("<tr><td>" + val2.key + "</td><td>" + val2.value + "</td></tr>");
              });
            }
            
            if (val.addlData.annotations.length !== 0) {
              items.push("<tr><th>Annotation</th><th>Time Offset</th></tr>");
            	
              $.each(val.addlData.annotations, function(key2, val2) {
                items.push("<tr><td>" + val2.annotation + "</td><td>" + val2.time + "</td></tr>");
              });
            }
              	
            items.push("</table>");
            items.push("</td>");
            id += 1;
          }
          
          items.push("</tr>");
              
          $("#trace").append(items.join(""));              
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
            <table id="trace">
              <caption>
              	<span id='caption' class='table-caption'>Trace ${id} started at<br></span>
              </caption>
              <tr><th>Time</th><th>Start</th><th>Service@Location</th><th>Name</th><th>Addl Data</th></tr>
              
			</table>
		  </div>
          
        </div>
      </div>    
    </div>
  </body>
</html>
