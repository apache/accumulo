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
    <#--title>${title} - Accumulo ${version}</title>
    <#if refresh > 0 ><meta http-equiv='refresh' content='${refresh}' /></#if>
    <meta http-equiv='Content-Type' content='test/html"' />
    <meta http-equiv='Content-Script-Type' content='text/javascript' />\n");
    <meta http-equiv='Content-Style-Type' content='text/css' />\n");
    <link rel='shortcut icon' type='image/jpg' href='/web/favicon.png' />\n");
    <link rel='stylesheet' type='text/css' href='/web/screen.css' media='screen' />\n");
    <script src='/web/functions.js' type='text/javascript'></script>\n");

    <!--[if lte IE 8]><script language="javascript" type="text/javascript" src="/web/flot/excanvas.min.js"></script><![endif]
    <script language="javascript" type="text/javascript" src="/web/flot/jquery.js"></script>
    <script language="javascript" type="text/javascript" src="/web/flot/jquery.flot.js"></script>
    -->
  </head>

  <body>
    <#--
    <div id='content-wrapper'>
      <div id='content'>
        <div id='header'><div id='headertitle'><h1>Master Server:localhost</h1></div>
        <div id='subheader'>Instance&nbsp;Name:&nbsp;monitor&nbsp;&nbsp;&nbsp;Version:&nbsp;1.8.0-SNAPSHOT
          <br><span class='smalltext'>Instance&nbsp;ID:&nbsp;2f904114-3313-4e29-9323-66330180b924</span>
          <br><span class='smalltext'>Wed&nbsp;Aug&nbsp;12&nbsp;23:13:08&nbsp;EDT&nbsp;2015</span></div>
        </div>


        <div id='main' style='bottom:0'>
        
          <br />
          <h2 class='warning'><a href='/log'>Log Events: 0 Errors, 3 Warnings, 3 Total</a></h2>
          <div>
            <a name='masterStatus'>&nbsp;</a>
            <table id='masterStatus' class='sortable'>
            <caption>
            <span class='table-caption'>Master&nbsp;Status</span><br />
            <a href='/op?action=toggleLegend&redir=%2Fmaster&page=/master&table=masterStatus&show=true'>Show&nbsp;Legend</a>
            </caption>
            <tr><th class='firstcell'>Master</th><th>#&nbsp;Online<br />Tablet&nbsp;Servers</th><th>#&nbsp;Total<br />Tablet&nbsp;Servers</th><th>Last&nbsp;GC</th><th>#&nbsp;Tablets</th><th>#&nbsp;Unassigned<br />Tablets</th><th>Entries</th><th>Ingest</th><th>Entries<br />Read</th><th>Entries<br />Returned</th><th>Hold&nbsp;Time</th><th>OS&nbsp;Load</th></tr>
            <tr class='highlight'><td class='firstcell left'>localhost</td><td class='right'>1</td><td class='right'>1</td><td class='left'><a href='/gc'>Waiting</a></td><td class='right'>4</td><td class='right'>0</td><td class='right'>2.20K</td><td class='right'>0</td><td class='right'>0</td><td class='right'>0</td><td class='right'>&mdash;</td><td class='right'>5.64</td></tr>
            </table>
          </div>
        
          <div>
            <a name='tableList'>&nbsp;</a>
            <table id='tableList' class='sortable'>
            <caption>
            <span class='table-caption'>Table&nbsp;List</span><br />
            <a href='/op?action=toggleLegend&redir=%2Fmaster&page=/master&table=tableList&show=true'>Show&nbsp;Legend</a>
            </caption>
            <tr><th class='firstcell'><a href='/op?action=sortTable&redir=%2Fmaster&page=/master&table=tableList&asc=false'>Table&nbsp;Name&nbsp;<img width='10px' height='10px' src='/web/up.gif' alt='v' /></a></th><th><a href='/op?action=sortTable&redir=%2Fmaster&page=/master&table=tableList&col=1'>State</a></th><th><a href='/op?action=sortTable&redir=%2Fmaster&page=/master&table=tableList&col=2'>#&nbsp;Tablets</a></th><th><a href='/op?action=sortTable&redir=%2Fmaster&page=/master&table=tableList&col=3'>#&nbsp;Offline<br />Tablets</a></th><th><a href='/op?action=sortTable&redir=%2Fmaster&page=/master&table=tableList&col=4'>Entries</a></th><th><a href='/op?action=sortTable&redir=%2Fmaster&page=/master&table=tableList&col=5'>Entries<br />In&nbsp;Memory</a></th><th><a href='/op?action=sortTable&redir=%2Fmaster&page=/master&table=tableList&col=6'>Ingest</a></th><th><a href='/op?action=sortTable&redir=%2Fmaster&page=/master&table=tableList&col=7'>Entries<br/>Read</a></th><th><a href='/op?action=sortTable&redir=%2Fmaster&page=/master&table=tableList&col=8'>Entries<br/>Returned</a></th><th><a href='/op?action=sortTable&redir=%2Fmaster&page=/master&table=tableList&col=9'>Hold&nbsp;Time</a></th><th><a href='/op?action=sortTable&redir=%2Fmaster&page=/master&table=tableList&col=10'>Running<br />Scans</a></th><th><a href='/op?action=sortTable&redir=%2Fmaster&page=/master&table=tableList&col=11'>Minor<br />Compactions</a></th><th><a href='/op?action=sortTable&redir=%2Fmaster&page=/master&table=tableList&col=12'>Major<br />Compactions</a></th></tr>
            <tr class='highlight'><td class='firstcell left'><a href='/tables?t=%210'>accumulo.metadata</a></td><td class='center'><span>ONLINE</span></td><td class='right'>2</td><td class='right'>0</td><td class='right'>28</td><td class='right'>3</td><td class='right'>0</td><td class='right'>0</td><td class='right'>0</td><td class='right'>&mdash;</td><td class='right'>0&nbsp;(0)</td><td class='right'>0&nbsp;(0)</td><td class='right'>0&nbsp;(0)</td></tr>
            <tr><td class='firstcell left'><a href='/tables?t=%2Brep'>accumulo.replication</a></td><td class='center'><span>OFFLINE</span></td><td class='right'>-</td><td class='right'>-</td><td class='right'>-</td><td class='right'>-</td><td class='right'>-</td><td class='right'>-</td><td class='right'>-</td><td class='right'>&mdash;</td><td class='right'>-</td><td class='right'>-</td><td class='right'>-</td></tr>
            <tr class='highlight'><td class='firstcell left'><a href='/tables?t=%2Br'>accumulo.root</a></td><td class='center'><span>ONLINE</span></td><td class='right'>1</td><td class='right'>0</td><td class='right'>6</td><td class='right'>6</td><td class='right'>0</td><td class='right'>0</td><td class='right'>0</td><td class='right'>&mdash;</td><td class='right'>0&nbsp;(0)</td><td class='right'>0&nbsp;(0)</td><td class='right'>0&nbsp;(0)</td></tr>
            <tr><td class='firstcell left'><a href='/tables?t=1'>trace</a></td><td class='center'><span>ONLINE</span></td><td class='right'>1</td><td class='right'>0</td><td class='right'>2.16K</td><td class='right'>0</td><td class='right'>0</td><td class='right'>0</td><td class='right'>0</td><td class='right'>&mdash;</td><td class='right'>0&nbsp;(0)</td><td class='right'>0&nbsp;(0)</td><td class='right'>0&nbsp;(0)</td></tr>
            </table>
          </div>
        </div>
      </div>
    </div>
    -->
  </body>
</html>