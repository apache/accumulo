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
        <div id='nav'>
          <span id='nav-title'><a href='/'>Overview</a></span><br />
          <hr />
          <a href='/master'>Master&nbsp;Server</a><br />
          <a href='/tservers'>Tablet&nbsp;Servers</a><br />
          <a href='/scans'>Active&nbsp;Scans</a><br />
          <a href='/vis'>Server Activity</a><br />
          <a href='/gc'>Garbage&nbsp;Collector</a><br />
          <a href='/tables'>Tables</a><br />
          <a href='/trace/summary?minutes=10'>Recent&nbsp;Traces</a><br />
          <a href='/replication'>Replication</a><br />
          {{ recent_logs_warning }}
          <span class='warning'><a href='/log'>Recent&nbsp;Logs&nbsp;<span class='smalltext'>(3)</a></span></span><br />
          <hr />
          <a href='/xml'>XML</a><br />
          <a href='/json'>JSON</a><hr />
          <div class='smalltext'>[<a href='/op?action=refresh&value=5&redir=%2Fmaster'>enable&nbsp;auto-refresh</a>]</div>
        </div>