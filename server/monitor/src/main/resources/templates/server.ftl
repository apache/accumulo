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
         * Creates server initial tables, passes values from template
         */
        $(document).ready(function() {
          createDetailHeader('${server}');
          createHistoryHeader();
          createCurrentHeader();
          createResultsHeader();
          refreshServer('${server}');
        });
      </script>
      <div><h3>${title}</h3></div>
      <div class="center-block">
        <table id="tServerDetail" class="table table-bordered table-striped table-condensed">

        </table>
      </div>
      <div class="center-block">
        <table id="opHistoryDetails" class="table table-bordered table-striped table-condensed">

        </table>
      </div>
      <div class="center-block">
        <table id="currentTabletOps" class="table table-bordered table-striped table-condensed">

        </table>
      </div>
      <div class="center-block">
        <table id="perTabletResults" class="table table-bordered table-striped table-condensed">

        </table>
      </div>
