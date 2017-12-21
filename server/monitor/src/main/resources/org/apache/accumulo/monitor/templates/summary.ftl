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
      <script>
        /**
         * Creates trace summary initial table, passes the minutes from the template
         */
        $(document).ready(function() {
          minutes = '${minutes}';
          refreshSummary();

          // Create tooltip for table column information
          $(document).tooltip();
        });
      </script>

      <div><h3>${title}</h3></div>
      <div class="center-block">
        <table id="traceSummary" class="table table-bordered table-striped table-condensed">
            <tbody><tr><th class="firstcell" title="Trace Type">Type&nbsp;</th>
                <th title="Number of spans of this type">Total&nbsp;</th>
                <th title="Shortest span duration">min&nbsp;</th>
                <th title="Longest span duration">max&nbsp;</th>
                <th title="Average span duration">avg&nbsp;</th>
                <th title="Counts of spans of different duration. Columns start at milliseconds, and each column is ten times longer: tens of milliseconds, seconds, tens of seconds, etc.">Histogram&nbsp;</th>
            </tr>
            </tbody>
        </table>
      </div>
