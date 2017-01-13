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
    
    <link rel="stylesheet" href="//code.jquery.com/ui/1.12.1/themes/base/jquery-ui.css">
    <script src="https://code.jquery.com/jquery-1.12.4.js"></script>
    
    <script src="https://ajax.googleapis.com/ajax/libs/jquery/1.12.4/jquery.min.js"></script>
    <!-- Latest compiled and minified CSS -->
    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css" integrity="sha384-BVYiiSIFeK1dGmJRAkycuHAHRg32OmUcww7on3RYdg4Va+PmSTsz/K68vbdEjh4u" crossorigin="anonymous">
    <!-- Optional theme -->
    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap-theme.min.css" integrity="sha384-rHyoN1iRsVXV4nD0JutlnGaslCJuC7uwjduW9SVrLvRYooPp2bWYgmgJQIXwl/Sp" crossorigin="anonymous">
    <!-- Latest compiled and minified JavaScript -->
    <script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/js/bootstrap.min.js" integrity="sha384-Tc5IQib027qvyjSMfHjOMaLkfuWVxZxUPnCJA7l2mCWNIpG9mGCD8wGNIcPD7Txa" crossorigin="anonymous"></script>

    <script src="https://code.jquery.com/ui/1.12.1/jquery-ui.js"></script>
    
    <script language="javascript" type="text/javascript">
        function getData() {
            $.getJSON("rest/tables", function(data) {
                sessionStorage.tables = JSON.stringify(data);
            });
        }    
        
        function populateTable(ns) {
			getData(); // Implement this better
            var data = JSON.parse(sessionStorage.tables);
            var tmpArr = JSON.parse(sessionStorage.namespaces);
            sessionStorage.namespaceChanged = true;
            
            if (ns !== undefined) {
                if (tmpArr.indexOf(ns) == -1 && ns !== "*") {
                    if (tmpArr.indexOf("*") !== -1) {
                        tmpArr.splice(tmpArr.indexOf("*"),1);
                    }
                    tmpArr.push((ns === "" ? "-" : ns));                
                } else if (ns == "*") {
                    tmpArr = [];
                    tmpArr.push("*");
                } else {
                    tmpArr.splice(tmpArr.indexOf(ns),1);
                    if (tmpArr.length === 0) {
                        tmpArr.push("*");
                    }
                }
            }
            
            sessionStorage.namespaces = JSON.stringify(tmpArr);
            
            var all = '<a onclick=populateTable("*")>*&nbsp;(All&nbsp;Tables)</a>';
            $("#namespaces").html("");
            clearTable("tableList");

            $("<li/>", {
                html: all,
                id: "*",
                class: tmpArr.indexOf("*") !== -1 ? "active" : ""
            }).appendTo("#namespaces");
            
            var numTables = 0;
            
            $.each(data.tables, function(keyT, tab) {
                var namespace = [];
                  
                namespace.push("<a onclick=populateTable('" + (tab.namespace === "" ? "-" : tab.namespace) + "')>" + (tab.namespace === "" ? "-&nbsp;(DEFAULT)" : tab.namespace) + "</a>");
                  
                $("<li/>", {
                    html: namespace.join(""),
                    id: tab.namespace === "" ? "-" : tab.namespace,
                    class: tmpArr.indexOf(tab.namespace === "" ? "-" : tab.namespace) !== -1 ? "active" : ""
                }).appendTo("#namespaces"); 
                  
                if (tmpArr.indexOf(tab.namespace === "" ? "-" : tab.namespace) !== -1 || tmpArr.indexOf("*") !== -1) {
                    $.each(tab.table, function(key, val) {
                        
                        var row = [];
                        row.push("<td class='firstcell left' data-value='" + val.tablename + "'><a href='/tables/" + val.tableId + "'>" + val.tablename + "</a></td>");
                        row.push("<td class='center' data-value='" + val.tableState + "'><span>" + val.tableState + "</span></td>");

                        if (val.tableState === "ONLINE") {
                            row.push("<td class='right' data-value='" + val.tablets + "'>" + bigNumberForQuantity(val.tablets) + "</td>");
                            row.push("<td class='right' data-value='" + val.offlineTablets + "'>" + bigNumberForQuantity(val.offlineTablets) + "</td>");
                            row.push("<td class='right' data-value='" + val.recs + "'>" + bigNumberForQuantity(val.recs) + "</td>");
                            row.push("<td class='right' data-value='" + val.recsInMemory + "'>" + bigNumberForQuantity(val.recsInMemory) + "</td>");
                            row.push("<td class='right' data-value='" + val.ingest + "'>" + bigNumberForQuantity(Math.floor(val.ingest)) + "</td>");
                            row.push("<td class='right' data-value='" + val.entriesRead + "'>" + bigNumberForQuantity(Math.floor(val.entriesRead)) + "</td>");
                            row.push("<td class='right' data-value='" + val.entriesReturned + "'>" + bigNumberForQuantity(Math.floor(val.entriesReturned)) + "</td>");
                            row.push("<td class='right' data-value='" + val.holdTime + "'>" + timeDuration(val.holdTime) + "</td>");
                            row.push("<td class='right' data-value='" + (val.scans.running + val.scans.queued) + "'>" + bigNumberForQuantity(val.scans.running) + "&nbsp;(" + val.scans.queued + ")</td>");
                            row.push("<td class='right' data-value='" + (val.minorCompactions.running + val.minorCompactions.queued) + "'>" + bigNumberForQuantity(val.minorCompactions.running) + "&nbsp;(" + val.minorCompactions.queued + ")</td>");
                            row.push("<td class='right' data-value='" + (val.majorCompactions.running + val.majorCompactions.queued) + "'>" + bigNumberForQuantity(val.majorCompactions.running) + "&nbsp;(" + val.majorCompactions.queued + ")</td>");
                        } else {
                            row.push("<td class='right' data-value='-'>-</td>");
                            row.push("<td class='right' data-value='-'>-</td>");
                            row.push("<td class='right' data-value='-'>-</td>");
                            row.push("<td class='right' data-value='-'>-</td>");
                            row.push("<td class='right' data-value='-'>-</td>");
                            row.push("<td class='right' data-value='-'>-</td>");
                            row.push("<td class='right' data-value='-'>-</td>");
                            row.push("<td class='right' data-value='-'>&mdash;</td>");
                            row.push("<td class='right' data-value='-'>-</td>");
                            row.push("<td class='right' data-value='-'>-</td>");
                            row.push("<td class='right' data-value='-'>-</td>");
                        }
        
                        $("<tr/>", {
                            html: row.join(""),
                            id: tab.namespace === "" ? "-" : tab.namespace
                        }).appendTo("#tableList");
                        
                        numTables++;
                    });
                }
            });
            $.each(data.tables, function(keyT, tab) {
                if (numTables === 0) {
                    if (tab.table.length === 0) {
                        var item = "<td class='center' colspan='13'><i>Empty</i></td>";
                        
                        $("<tr/>", {
                            html: item,
                            id: tab.namespace === "" ? "-" : tab.namespace
                        }).appendTo("#tableList");
                    }
                }
            });
            sortTable(sessionStorage.tableColumnSort === undefined ? 0 : sessionStorage.tableColumnSort);
        }
        
        function sortTable(n) {
            if (!JSON.parse(sessionStorage.namespaceChanged)) {
                if (sessionStorage.tableColumnSort !== undefined && sessionStorage.tableColumnSort == n && sessionStorage.direction !== undefined) {
                    direction = sessionStorage.direction === "asc" ? "desc" : "asc";
                }
            } else {
                direction = sessionStorage.direction === undefined ? "asc" : sessionStorage.direction;
            }
            
            sessionStorage.tableColumnSort = n;
            
            sortTables("tableList", direction, n);
            sessionStorage.namespaceChanged = false;
        }
        
        $(function() {
            $(document).tooltip();
        });

        function createHeader() {	
            
            var caption = [];

            caption.push("<span class='table-caption'>Table&nbsp;List</span><br />");

            $("<caption/>", {
                html: caption.join("")
            }).appendTo("#tableList");
            
            var items = [];
                        
            items.push("<th class='firstcell' onclick='sortTable(0)'>Table&nbsp;Name&nbsp;</th>");
            items.push("<th onclick='sortTable(1)'>State&nbsp;</th>");
            items.push("<th onclick='sortTable(2)' title='"+getDescription(2)+"'>#&nbsp;Tablets&nbsp;</th>");
            items.push("<th onclick='sortTable(3)' title='"+getDescription(3)+"'>#&nbsp;Offline<br />Tablets&nbsp;</th>");
            items.push("<th onclick='sortTable(4)' title='"+getDescription(4)+"'>Entries&nbsp;</th>");
            items.push("<th onclick='sortTable(5)' title='"+getDescription(5)+"'>Entries<br />In&nbsp;Memory&nbsp;</th>");
            items.push("<th onclick='sortTable(6)' title='"+getDescription(6)+"'>Ingest&nbsp;</th>");
            items.push("<th onclick='sortTable(7)' title='"+getDescription(7)+"'>Entries<br />Read&nbsp;</th>");
            items.push("<th onclick='sortTable(8)' title='"+getDescription(8)+"'>Entries<br />Returned&nbsp;</th>");
            items.push("<th onclick='sortTable(9)' title='"+getDescription(9)+"'>Hold&nbsp;Time&nbsp;</th>");
            items.push("<th onclick='sortTable(10)' title='"+getDescription(10)+"'>Running<br />Scans&nbsp;</th>");
            items.push("<th onclick='sortTable(11)' title='"+getDescription(11)+"'>Minor<br />Compactions&nbsp;</th>");
            items.push("<th onclick='sortTable(12)' title='"+getDescription(12)+"'>Major<br />Compactions&nbsp;</th>");
            
            $("<tr/>", {
                html: items.join("")
             }).appendTo("#tableList");
        }
        
        function clearTable(tableID) {
            
            $("#" + tableID).find("tr:not(:first)").remove();
        }
        
        function getDescription(n) {
            
            switch(n) {
                case 2:
                    return "Tables are broken down into ranges of rows called tablets.";
                case 3:
                    return "Tablets unavailable for query or ingest. May be a transient condition when tablets are moved for balancing.";
                case 4:
                    return "Key/value pairs over each instance, table or tablet.";
                case 5:
                    return "The total number of key/value pairs stored in memory and not yet written to disk.";
                case 6:
                    return "The number of Key/Value pairs inserted. Note that deletes are 'inserted'.";
                case 7:
                    return "The number of Key/Value pairs read on the server side. Not all key values read may be returned to client because of filtering.";
                case 8:
                    return "The number of Key/Value pairs returned to clients during queries. This is not the number of scans.";
                case 9:
                    return "The amount of time that ingest operations are suspended while waiting for data to be written to disk.";
                case 10:
                    return "Information about the scans threads. Shows how many threads are running and how much work is queued for the threads.";
                case 11:
                    return "Flushing memory to disk is called a \"Minor Compaction.\" Multiple tablets can be minor compacted simultaneously, but sometimes they must wait for resources to be available. These tablets that are waiting for compaction are \"queued\" and are indicated using parentheses. So 2 (3) indicates there are two compactions running and three queued waiting for resources.";
                case 12:
                    return "Gathering up many small files and rewriting them as one larger file is called a \"Major Compaction\". Major Compactions are performed as a consequence of new files created from Minor Compactions and Bulk Load operations. They reduce the number of files used during queries.";
                default:
                    return "Not valid";
            }
        }

    </script>
  </head>

  <body>
  	<script type="text/javascript">

		$(document).ready(function() {
			$.ajaxSetup({
				async: false
			});
            getData();
            $.ajaxSetup({
				async: true
			});
            createHeader();
            if (sessionStorage.namespaces === undefined) {
                sessionStorage.namespaces = "[]";
                populateTable("*");
            }
			populateTable(undefined);
        });        
        
  	</script>  	
    <div id='content-wrapper'>
      <div id='content'>
        <#include "/templates/header.ftl">
        <#include "/templates/sidebar.ftl">

        <div id='main' style='bottom:0'>
          <div id="filters">
			<div class="table-caption">Namespaces</div>
			<hr />
			<div class='left show'>
			  <ul id="namespaces" class="nav nav-pills nav-stacked" role="tablist">
			    
			  </ul>
			</div>
		  </div>
          <div id="tables">
            <table id='tableList' class='table table-bordered table-striped table-condensed'>
              
            </table>
          </div>
        </div>
      </div>    
    </div>
  </body>
</html>
