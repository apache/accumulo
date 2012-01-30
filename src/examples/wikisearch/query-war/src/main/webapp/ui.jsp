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
<%@page contentType="text/html" pageEncoding="UTF-8"%>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN"
   "http://www.w3.org/TR/html4/loose.dtd">

<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <title>Wiki Search Page</title>
        <style type="text/css">
        	#comments {
				width: 90%;
				margin: auto;
        	}
        	h1 {
        		text-align: center;
        	}
			#d {
				width: 80%;
				margin: auto;
			}
			.center_box {
				width: 70%;
				margin: auto;
			}
			.center_input {
				width: 30%;
				margin: auto;
			}
        </style>
    </head>
    <body>
    	<div id="comments">
    		<h1>Wiki Search using Apache Accumulo</h1>
    		<p>This sample application demonstrates the ability to use search documents using Apache Accumulo. The associated ingest software
    		extracts the id, title, timestamp, and comments from each wikipedia article. In addition, the wikipedia text has been tokenized
    		and is available for searching. You can enter a boolean expression into the search box below and select the particular set of
    		wikipedia languages you want to search.</p>
    		<p> Fields available for searching:
    		<ol>
    			<li>TEXT</li>
    			<li>ID</li>
    			<li>TITLE</li>
    			<li>TIMESTAMP</li>
    			<li>COMMENTS</li>
    		</ol>
    		<p>The search syntax is boolean logic, for example: TEXT == 'boy' and TITLE =~ 'Autism'. The supported operators are:
    		==, !=, &lt;, &gt;, &le;, &ge;, =~, and !~. Likewise grouping can be performed using parentheses and predicates can be
    		joined using and, or, and not.
    		<p>To highlight the cell-level access control of Apache Accumulo, the "authorization" required for a particular cell is the language 
    		of the associated wikipedia article.
    	</div>
    	<div id="d">
	    	<form id="FORM" name="queryForm" method="get" target="results" onsubmit="return setAction()">
	    		<br />
	    		<br />
	    		<div class="center_box">
	    		<label>Query: </label>
	    		<input id="QUERY" type="text" name="query" size="100" maxlength="300"/>
	    		</div>
	    		<br />
	    		<div class="center_input">
	    		<label>Authorizations: </label>
	    		<br />
	    		<label>All</label><input type="checkbox" name="auths" value="all" />
				</div>
	    		<div class="center_input">
				<label>Arabic</label> <input type="checkbox" name="auths" value="arwiki" />
				<label>Brazilian</label> <input type="checkbox" name="auths" value="brwiki" />
				<label>Chinese</label> <input type="checkbox" name="auths" value="zhwiki" />
				</div>
				<div class="center_input">
				<label>Dutch</label> <input type="checkbox" name="auths" value="nlwiki" />
	    		<label>English</label> <input type="checkbox" name="auths" value="enwiki" />
				<label>Farsi</label> <input type="checkbox" name="auths" value="fawiki" />
				</div>
	    		<div class="center_input">				
				<label>French</label> <input type="checkbox" name="auths" value="frwiki" />
				<label>German</label> <input type="checkbox" name="auths" value="dewiki" />
				<label>Greek</label> <input type="checkbox" name="auths" value="elwiki" />
				</div>
	    		<div class="center_input">				
				<label>Italian</label> <input type="checkbox" name="auths" value="itwiki" />
				<label>Spanish</label> <input type="checkbox" name="auths" value="eswiki" />
				<label>Russia</label>n <input type="checkbox" name="auths" value="ruwiki" /><br />
				</div>
	    		<div class="center_input">				
				<input type="submit" name="Submit Query" />
				</div>
	    	</form>
	   		<br />
	   		<br />
	    	<iframe name="results" width="90%" height="400" scrolling="yes" >
	    	</iframe>
    	</div>
    	<script type="text/javascript">
    		function setAction() {
	    		var f = document.forms[0];
	    		var authString = "";
	    		var sep = "";
	    		for (var i=0; i<f.auths.length; i++) {
	    			if (f.auths[i].checked) {
	    				authString = authString + sep + f.auths[i].value;
	    				sep = ",";
	    			}
	    		}
	    		//Build the new query
				var existingAction = "/accumulo-wikisearch/rest/Query/html";
	    		var query = f.query.value;
	    		
	    		var newAction = existingAction + "?query=" + query + "&auths=" + authString;
	    		document.forms[0].action = newAction;
    		}
    	</script>    	
    </body>
</html>
