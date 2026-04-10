<#--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->
    <div id="managerRunningBanner" style="display: none;">
        <div class="alert alert-danger" role="alert">Manager Server Not Running</div>
    </div>
    <div id="managerStateBanner" style="display: none;">
        <div id="manager-banner-message" class="alert alert-warning" role="alert"></div>
    </div>
    <table id="managers" class="table caption-top table-bordered table-striped table-condensed">
        <caption><span class="table-caption">Managers</span><br /></caption>
        <tbody></tbody>
    </table>
    <br />
    <table id="managers_compactions" class="table caption-top table-bordered table-striped table-condensed">
        <caption><span class="table-caption">Manager Compaction Activity</span><br /></caption>
        <tbody></tbody>
    </table>
    <br />
    <table id="managers_fate" class="table caption-top table-bordered table-striped table-condensed">
        <caption><span class="table-caption">Manager Fate Activity</span><br /></caption>
        <tbody></tbody>
    </table>
    <br />
    <!--
    <table id="recoveryList" class="table caption-top table-bordered table-striped table-condensed">
        <caption><span class="table-caption">Log&nbsp;Recovery</span><br />
            <span class="table-subcaption">Some tablets were unloaded in an unsafe manner. Write-ahead logs are being
                recovered.</span><br />
        </caption>
        <thead>
            <tr>
                <th>Server</th>
                <th>Log</th>
                <th class="duration">Time</th>
                <th class="percent">Progress</th>
            </tr>
        </thead>
        <tbody></tbody>
    </table>
    <br />
    -->