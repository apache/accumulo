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
      <style>
        td.right { text-align: right }
        table.indent { position: relative; left: 10% }
        td.left { text-align: left }
      </style>
      <script type="text/javascript">
        /**
         * Creates trace id initial table, passes id from template
         */
        $(document).ready(function() {

          id = '${id}';
          refreshTraceShow();

          // Create tooltip for table column information
          $(document).tooltip();
        });

        /**
         * Toggles row
         *
         * @param {string} id Row id to toggle
         */
        function toggle(id) {
          var elt = document.getElementById(id);
          if (elt.style.display=='none') {
            elt.style.display='table-row';
          } else { 
            elt.style.display='none';
          }
        }

        /**
         * Selects where to display the row
         */
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
      </script>
      <div><h3>${title}</h3></div>
      <div class="center-block">
        <table id="trace" class="table table-bordered table-striped table-condensed">
            <caption><span id="caption" class="table-caption">Trace ${id} started at<br/></span></caption>
            <tbody>
            <tr><th>Time&nbsp;</th><th>Start&nbsp;</th><th>Service@Location&nbsp;</th><th>Name&nbsp;</th><th>Addl&nbsp;Data&nbsp;</th></tr>
            </tbody>
        </table>
      </div>
