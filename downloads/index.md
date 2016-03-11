---
title: Downloads
nav: nav_downloads
---

<script type="text/javascript">
/**
* Function that tracks a click on an outbound link in Google Analytics.
* This function takes a valid URL string as an argument, and uses that URL string
* as the event label.
*/
var gaCallback = function(event) {
  var hrefUrl = event.target.getAttribute('href')
  if (event.ctrlKey || event.shiftKey || event.metaKey || event.which == 2) {
    var newWin = true;}

  // $(this) != this
  var url = "http://accumulo.apache.org" + $(this).attr("id")
  if (newWin) {
    ga('send', 'event', 'outbound', 'click', url, {'nonInteraction': 1});
    return true;
  } else {
    ga('send', 'event', 'outbound', 'click', url, {'hitCallback':
    function () {window.location.href = hrefUrl;}}, {'nonInteraction': 1});
    return false;
  }
};

$( document ).ready(function() {
  if (ga.hasOwnProperty('loaded') && ga.loaded === true) {
    $('.download_external').click(gaCallback);
  }
});

var createSection = function(name, items, divider) {
  var section = '';
  if (divider == undefined) { divider = true; }
  if (divider) {
    section += '<li class="divider" <="" li=""> </li>';
  }
  section += '<li class="dropdown-header">' + name + '</li>';
  for (var i = 0; i < items.length; i++) {
    section += '<li><a href="#">' + items[i] + '</a></li>';
  }
  return section;
};

var updateLinks = function(mirror) {
  $('a[link-suffix]').each(function(i, obj) {
    $(obj).attr('href', mirror.replace(/\/+$/, "") + $(obj).attr('link-suffix'));
  });
};

var mirrorsCallback = function(json) {
  var mirrorSelection = $("#mirror_selection");
  var htmlContent =  '<span class="help-block">Select a mirror:</span>' +
    '<div class="btn-group">' +
      '<button type="button" class="btn btn-default dropdown-toggle" data-toggle="dropdown">' +
        '<span data-bind="label">' + json.preferred + '</span>&nbsp;<span class="caret">' +
      '</button>' +
      '<ul class="dropdown-menu">';

  htmlContent += createSection("Preferred Mirror (based on location)", [ json.preferred ], false);
  htmlContent += createSection("HTTP Mirrors", json.http);
  htmlContent += createSection("FTP Mirrors", json.ftp);
  htmlContent += createSection("Backup Mirrors", json.backup);

  htmlContent += '</ul></div>';
  mirrorSelection.html(htmlContent);

  $("#mirror_selection a").click(function(event) {
      var target=$(event.target);
      var mirror=target.text();
      updateLinks(mirror);
      target.closest('.btn-group').find('[data-bind="label"]').text(mirror).end();
  });

  updateLinks(json.preferred);
};

// get mirrors when page is ready
var mirrorURL = "/mirrors.cgi"; // http[s]://accumulo.apache.org/mirrors.cgi
$(function() { $.getJSON(mirrorURL + "?as_json", mirrorsCallback); });

</script>

<div id="mirror_selection"></div>
<br />

Be sure to verify your downloads by these [procedures][VERIFY_PROCEDURES] using these [KEYS][GPG_KEYS].

## Current Releases

### 1.7.1 <span class="label label-primary">latest</span>

The most recent Apache Accumulo&trade; release is version 1.7.1. See the [release notes][REL_NOTES_17] and [CHANGES][CHANGES_17].

For convenience, [MD5][MD5SUM_17] and [SHA1][SHA1SUM_17] hashes are also available.

<table class="table">
<tr>
<th>Generic Binaries</th>
<td><a href="https://www.apache.org/dyn/closer.lua/accumulo/1.7.1/accumulo-1.7.1-bin.tar.gz" link-suffix="/accumulo/1.7.1/accumulo-1.7.1-bin.tar.gz" class="download_external" id="/downloads/accumulo-1.7.1-bin.tar.gz">accumulo-1.7.1-bin.tar.gz</a></td>
<td><a href="https://www.apache.org/dist/accumulo/1.7.1/accumulo-1.7.1-bin.tar.gz.asc">ASC</a></td>
</tr>
<tr>
<th>Source</th>
<td><a href="https://www.apache.org/dyn/closer.lua/accumulo/1.7.1/accumulo-1.7.1-src.tar.gz" link-suffix="/accumulo/1.7.1/accumulo-1.7.1-src.tar.gz" class="download_external" id="/downloads/accumulo-1.7.1-src.tar.gz">accumulo-1.7.1-src.tar.gz</a></td>
<td><a href="https://www.apache.org/dist/accumulo/1.7.1/accumulo-1.7.1-src.tar.gz.asc">ASC</a></td>
</tr>
</table>

#### 1.7 Documentation
* <a href="https://github.com/apache/accumulo/blob/rel/1.7.1/README.md" class="download_external" id="/1.7/README">README</a>
* [HTML User Manual][MANUAL_HTML_17]
* [Examples][EXAMPLES_17]
* <a href="/1.7/apidocs" class="download_external" id="/1.7/apidocs">Javadoc</a>

### 1.6.5

The most recent 1.6.x release of Apache Accumulo&trade; is version 1.6.5. See the [release notes][REL_NOTES_16] and [CHANGES][CHANGES_16].

For convenience, [MD5][MD5SUM_16] and [SHA1][SHA1SUM_16] hashes are also available.

<table class="table">
<tr>
<th>Generic Binaries</th>
<td><a href="https://www.apache.org/dyn/closer.lua/accumulo/1.6.5/accumulo-1.6.5-bin.tar.gz" link-suffix="/accumulo/1.6.5/accumulo-1.6.5-bin.tar.gz" class="download_external" id="/downloads/accumulo-1.6.5-bin.tar.gz">accumulo-1.6.5-bin.tar.gz</a></td>
<td><a href="https://www.apache.org/dist/accumulo/1.6.5/accumulo-1.6.5-bin.tar.gz.asc">ASC</a></td>
</tr>
<tr>
<th>Source</th>
<td><a href="https://www.apache.org/dyn/closer.lua/accumulo/1.6.5/accumulo-1.6.5-src.tar.gz" link-suffix="/accumulo/1.6.5/accumulo-1.6.5-src.tar.gz" class="download_external" id="/downloads/accumulo-1.6.5-src.tar.gz">accumulo-1.6.5-src.tar.gz</a></td>
<td><a href="https://www.apache.org/dist/accumulo/1.6.5/accumulo-1.6.5-src.tar.gz.asc">ASC</a></td>
</tr>
</table>

#### 1.6 Documentation
* <a href="https://git-wip-us.apache.org/repos/asf?p=accumulo.git;a=blob_plain;f=README;hb=rel/1.6.5" class="download_external" id="/1.6/README">README</a>
* <a href="http://search.maven.org/remotecontent?filepath=org/apache/accumulo/accumulo-docs/1.6.5/accumulo-docs-1.6.5-user-manual.pdf" class="download_external" id="/1.6/accumulo_user_manual.pdf">PDF manual</a>
* [html manual][MANUAL_HTML_16]
* [examples][EXAMPLES_16]
* <a href="/1.6/apidocs" class="download_external" id="/1.6/apidocs">Javadoc</a>

## Older releases

Older releases can be found in the [archives][ARCHIVES].


[VERIFY_PROCEDURES]: https://www.apache.org/info/verification.html "Verify"
[GPG_KEYS]: https://www.apache.org/dist/accumulo/KEYS "KEYS"
[ARCHIVES]: https://archive.apache.org/dist/accumulo/

[MANUAL_HTML_16]: /1.6/accumulo_user_manual.html "1.6 user manual"
[MANUAL_HTML_17]: /1.7/accumulo_user_manual.html "1.7 user manual"

[EXAMPLES_16]: /1.6/examples "1.6 examples"
[EXAMPLES_17]: /1.7/examples "1.7 examples"

[CHANGES_16]: https://issues.apache.org/jira/browse/ACCUMULO/fixforversion/12333674 "1.6.5 CHANGES"
[CHANGES_17]: https://issues.apache.org/jira/browse/ACCUMULO/fixforversion/12329940 "1.7.1 CHANGES"

[REL_NOTES_16]: /release_notes/1.6.5.html "1.6.5 Release Notes"
[REL_NOTES_17]: /release_notes/1.7.1.html "1.7.1 Release Notes"

[MD5SUM_16]: https://www.apache.org/dist/accumulo/1.6.5/MD5SUM "1.6.5 MD5 file hashes"
[MD5SUM_17]: https://www.apache.org/dist/accumulo/1.7.1/MD5SUM "1.7.1 MD5 file hashes"

[SHA1SUM_16]: https://www.apache.org/dist/accumulo/1.6.5/SHA1SUM "1.6.5 SHA1 file hashes"
[SHA1SUM_17]: https://www.apache.org/dist/accumulo/1.7.1/SHA1SUM "1.7.1 SHA1 file hashes"
