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
      <div><h3>${title}</h3></div>
      <div class="center-block">
        <table id="masterBulkImportStatus" class="table table-bordered table-striped table-condensed">
            <caption><span class="table-caption">All Bulk Imports</span><br/></caption>
            <tbody>
            <tr><th class="firstcell">Directory&nbsp;</th>
                <th title="The age of the import.">Age&nbsp;</th>
                <th title="The current state of the bulk import">State&nbsp;</th></tr>
            </tbody>
        </table>
      </div>

      <div class="center-block">
        <table id="bulkImportStatus" class="table table-bordered table-striped table-condensed">
            <caption><span class="table-caption">Per TabletServer</span><br/></caption>
            <tbody><tr><th class="firstcell">Server&nbsp;</th>
                <th title="Number of imports presently running">#&nbsp;</th>
                <th title="The age of the oldest import running on this server.">Oldest&nbsp;Age&nbsp;</th>
            </tr>
            </tbody>
        </table>
      </div>
