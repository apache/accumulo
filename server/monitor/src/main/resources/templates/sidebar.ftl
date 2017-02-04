<#--
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
          <a href='/bulkImports'>Bulk&nbsp;Imports</a><br />
          <a href='/vis'>Server&nbsp;Activity</a><br />
          <a href='/gc'>Garbage&nbsp;Collector</a><br />
          <a href='/tables'>Tables</a><br />
          <a href='/trace/summary?minutes=10'>Recent&nbsp;Traces</a><br />
          <a href='/replication'>Replication</a><br />
          <#if num_logs gt 0>
            <span class='<#if logsHaveError?? && logsHaveError>error<#else>warning</#if>'><a href='/log'>Recent&nbsp;Logs&nbsp;<span class='smalltext'>(${num_logs})</span></a></span><br />
          </#if>
          <#if num_problems gt 0>
            <span class='error'><a href='/problems'>Table&nbsp;Problems&nbsp;<span class='smalltext'>(${num_problems}")</span></a></span><br />
          </#if>
          <hr />
          <a href='/xml'>XML</a><br />
          <a href='/rest/json'>JSON</a><hr />
          <#if is_ssl>
            <a href='/shell'>Shell</a><hr />
          </#if>
          <div class='smalltext'>[<a href='/op?action=refresh&value=<#if refresh < 1>5<#else>-1</#if><#if redirect??>&redir=${redirect}</#if>'>
                <#if refresh < 1>enable<#else>disable</#if>&nbsp;auto-refresh</a>]</div><hr />
          <div class='smalltext'><a href='https://accumulo.apache.org/' target='_blank'>Apache&nbsp;Accumulo</a></div>
        </div>
