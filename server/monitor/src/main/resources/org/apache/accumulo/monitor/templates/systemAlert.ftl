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
<div id="systemAlert" class="alert position-relative" style="display: none;" role="alert">
    <button id="systemAlertCloseButton" type="button" class="btn-close position-absolute m-2 top-0 end-0" aria-label="Close"></button>
    <div class="d-flex">
        <h3 class="d-flex align-items-center me-3">
            <span class="bi bi-exclamation-triangle-fill"></span>
        </h3>
        <ul id="alertMessages" class="mb-0 flex-grow-1">
            <!-- Messages will be appended here -->
        </ul>
    </div>
</div>