---
title: Downloads
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
  var url = window.location.protocol + "//accumulo.apache.org" + $(this).attr("id")
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

var updateLinks = function(mirror) {
  $('a[link-suffix]').each(function(i, obj) {
    $(obj).attr('href', mirror.replace(/\/+$/, "") + $(obj).attr('link-suffix'));
  });
};

var mirrorsCallback = function(json) {
  var htmlContent = '<div class="row"><div class="col-md-3"><h5>Select an Apache download mirror:</h5></div>' +
    '<div class="col-md-5"><select class="form-control" id="apache-mirror-select">';
  htmlContent += '<optgroup label="Preferred Mirror (based on location)">';
  htmlContent += '<option selected="selected">' + json.preferred + '</option>';
  htmlContent += '</optgroup>';
  htmlContent += '<optgroup label="HTTP Mirrors">';
  for (var i = 0; i < json.http.length; i++) {
    htmlContent += '<option>' + json.http[i] + '</option>';
  }
  htmlContent += '</optgroup>';
  htmlContent += '<optgroup label="FTP Mirrors">';
  for (var i = 0; i < json.ftp.length; i++) {
    htmlContent += '<option>' + json.ftp[i] + '</option>';
  }
  htmlContent += '</optgroup>';
  htmlContent += '<optgroup label="Backup Mirrors">';
  for (var i = 0; i < json.backup.length; i++) {
    htmlContent += '<option>' + json.backup[i] + '</option>';
  }
  htmlContent += '</optgroup>';
  htmlContent += '</select></div></div>';

  $("#mirror_selection").html(htmlContent);

  $( "#apache-mirror-select" ).change(function() {
    var mirror = $("#apache-mirror-select option:selected").text();
    updateLinks(mirror);
  });

  updateLinks(json.preferred);
};

// get mirrors when page is ready
var mirrorURL = window.location.protocol + "//accumulo.apache.org/mirrors.cgi"; // http[s]://accumulo.apache.org/mirrors.cgi
$(function() { $.getJSON(mirrorURL + "?as_json", mirrorsCallback); });

</script>

<div id="mirror_selection"></div>

Be sure to verify your downloads by these [procedures][VERIFY_PROCEDURES] using these [KEYS][GPG_KEYS].

## Current Releases

### 1.8.0 **latest**{: .label .label-primary }

The most recent Apache Accumulo&trade; release is version 1.8.0. See the [release notes][REL_NOTES_18] and [CHANGES][CHANGES_18].

For convenience, [MD5][MD5SUM_18] and [SHA1][SHA1SUM_18] hashes are also available.

{: .table }
| **Generic Binaries** | [accumulo-1.8.0-bin.tar.gz][BIN_18] | [ASC][ASC_BIN_18] |
| **Source**           | [accumulo-1.8.0-src.tar.gz][SRC_18] | [ASC][ASC_SRC_18] |

#### 1.8 Documentation
* [README][README_18]
* [HTML User Manual][MANUAL_HTML_18]
* [Examples][EXAMPLES_18]
* [Javadoc][JAVADOC_18]


### 1.7.2

The most recent 1.7.x release of Apache Accumulo&trade; is version 1.7.2. See the [release notes][REL_NOTES_17] and [CHANGES][CHANGES_17].

For convenience, [MD5][MD5SUM_17] and [SHA1][SHA1SUM_17] hashes are also available.

{: .table }
| **Generic Binaries** | [accumulo-1.7.2-bin.tar.gz][BIN_17] | [ASC][ASC_BIN_17] |
| **Source**           | [accumulo-1.7.2-src.tar.gz][SRC_17] | [ASC][ASC_SRC_17] |

#### 1.7 Documentation
* [README][README_17]
* [HTML User Manual][MANUAL_HTML_17]
* [Examples][EXAMPLES_17]
* [Javadoc][JAVADOC_17]

### 1.6.6

The most recent 1.6.x release of Apache Accumulo&trade; is version 1.6.6. See the [release notes][REL_NOTES_16] and [CHANGES][CHANGES_16].

For convenience, [MD5][MD5SUM_16] and [SHA1][SHA1SUM_16] hashes are also available.

{: .table }
| **Generic Binaries** | [accumulo-1.6.6-bin.tar.gz][BIN_16] | [ASC][ASC_BIN_16] |
| **Source**           | [accumulo-1.6.6-src.tar.gz][SRC_16] | [ASC][ASC_SRC_16] |

#### 1.6 Documentation
* [README][README_16]
* [PDF manual][MANUAL_PDF_16]
* [html manual][MANUAL_HTML_16]
* [examples][EXAMPLES_16]
* [Javadoc][JAVADOC_16]

## Older releases

Older releases can be found in the [archives][ARCHIVES].


[VERIFY_PROCEDURES]: https://www.apache.org/info/verification "Verify"
[GPG_KEYS]: https://www.apache.org/dist/accumulo/KEYS "KEYS"
[ARCHIVES]: https://archive.apache.org/dist/accumulo

[ASC_BIN_16]: https://www.apache.org/dist/accumulo/1.6.6/accumulo-1.6.6-bin.tar.gz.asc
[ASC_SRC_16]: https://www.apache.org/dist/accumulo/1.6.6/accumulo-1.6.6-src.tar.gz.asc

[ASC_BIN_17]: https://www.apache.org/dist/accumulo/1.7.2/accumulo-1.7.2-bin.tar.gz.asc
[ASC_SRC_17]: https://www.apache.org/dist/accumulo/1.7.2/accumulo-1.7.2-src.tar.gz.asc

[ASC_BIN_18]: https://www.apache.org/dist/accumulo/1.8.0/accumulo-1.8.0-bin.tar.gz.asc
[ASC_SRC_18]: https://www.apache.org/dist/accumulo/1.8.0/accumulo-1.8.0-src.tar.gz.asc

[BIN_16]: https://www.apache.org/dyn/closer.lua/accumulo/1.6.6/accumulo-1.6.6-bin.tar.gz
{: .download_external link-suffix="/accumulo/1.6.6/accumulo-1.6.6-bin.tar.gz" id="/downloads/accumulo-1.6.6-bin.tar.gz" }
[SRC_16]: https://www.apache.org/dyn/closer.lua/accumulo/1.6.6/accumulo-1.6.6-src.tar.gz
{: .download_external link-suffix="/accumulo/1.6.6/accumulo-1.6.6-src.tar.gz" id="/downloads/accumulo-1.6.6-src.tar.gz" }

[BIN_17]: https://www.apache.org/dyn/closer.lua/accumulo/1.7.2/accumulo-1.7.2-bin.tar.gz
{: .download_external link-suffix="/accumulo/1.7.2/accumulo-1.7.2-bin.tar.gz" id="/downloads/accumulo-1.7.2-bin.tar.gz" }
[SRC_17]: https://www.apache.org/dyn/closer.lua/accumulo/1.7.2/accumulo-1.7.2-src.tar.gz
{: .download_external link-suffix="/accumulo/1.7.2/accumulo-1.7.2-src.tar.gz" id="/downloads/accumulo-1.7.2-src.tar.gz" }

[BIN_18]: https://www.apache.org/dyn/closer.lua/accumulo/1.8.0/accumulo-1.8.0-bin.tar.gz
{: .download_external link-suffix="/accumulo/1.8.0/accumulo-1.8.0-bin.tar.gz" id="/downloads/accumulo-1.8.0-bin.tar.gz" }
[SRC_18]: https://www.apache.org/dyn/closer.lua/accumulo/1.8.0/accumulo-1.8.0-src.tar.gz
{: .download_external link-suffix="/accumulo/1.8.0/accumulo-1.8.0-src.tar.gz" id="/downloads/accumulo-1.8.0-src.tar.gz" }

[README_16]: https://git-wip-us.apache.org/repos/asf?p=accumulo.git;a=blob_plain;f=README;hb=rel/1.6.6
{: .download_external id="/1.6/README" }
[README_17]: https://github.com/apache/accumulo/blob/rel/1.7.2/README.md
{: .download_external id="/1.7/README" }
[README_18]: https://github.com/apache/accumulo/blob/rel/1.8.0/README.md
{: .download_external id="/1.8/README" }

[JAVADOC_16]: {{ site.baseurl }}/1.6/apidocs/
{: .download_external id="/1.6/apidocs/" }
[JAVADOC_17]: {{ site.baseurl }}/1.7/apidocs/
{: .download_external id="/1.7/apidocs/" }
[JAVADOC_18]: {{ site.baseurl }}/1.8/apidocs/
{: .download_external id="/1.8/apidocs/" }

[MANUAL_PDF_16]: https://search.maven.org/remotecontent?filepath=org/apache/accumulo/accumulo-docs/1.6.6/accumulo-docs-1.6.6-user-manual.pdf
{: .download_external id="/1.6/accumulo_user_manual.pdf" }
[MANUAL_HTML_16]: {{ site.baseurl }}/1.6/accumulo_user_manual "1.6 user manual"
[MANUAL_HTML_17]: {{ site.baseurl }}/1.7/accumulo_user_manual "1.7 user manual"
[MANUAL_HTML_18]: {{ site.baseurl }}/1.8/accumulo_user_manual "1.8 user manual"

[EXAMPLES_16]: {{ site.baseurl }}/1.6/examples "1.6 examples"
[EXAMPLES_17]: {{ site.baseurl }}/1.7/examples "1.7 examples"
[EXAMPLES_18]: {{ site.baseurl }}/1.8/examples "1.8 examples"

[CHANGES_16]: https://issues.apache.org/jira/browse/ACCUMULO/fixforversion/12334846 "1.6.6 CHANGES"
[CHANGES_17]: https://issues.apache.org/jira/browse/ACCUMULO/fixforversion/12333776 "1.7.2 CHANGES"
[CHANGES_18]: https://issues.apache.org/jira/browse/ACCUMULO/fixforversion/12329879 "1.8.0 CHANGES"

[REL_NOTES_16]: {{ site.baseurl }}/release_notes/1.6.6 "1.6.6 Release Notes"
[REL_NOTES_17]: {{ site.baseurl }}/release_notes/1.7.2 "1.7.2 Release Notes"
[REL_NOTES_18]: {{ site.baseurl }}/release_notes/1.8.0 "1.8.0 Release Notes"

[MD5SUM_16]: https://www.apache.org/dist/accumulo/1.6.6/MD5SUM "1.6.6 MD5 file hashes"
[MD5SUM_17]: https://www.apache.org/dist/accumulo/1.7.2/MD5SUM "1.7.2 MD5 file hashes"
[MD5SUM_18]: https://www.apache.org/dist/accumulo/1.8.0/MD5SUM "1.8.0 MD5 file hashes"

[SHA1SUM_16]: https://www.apache.org/dist/accumulo/1.6.6/SHA1SUM "1.6.6 SHA1 file hashes"
[SHA1SUM_17]: https://www.apache.org/dist/accumulo/1.7.2/SHA1SUM "1.7.2 SHA1 file hashes"
[SHA1SUM_18]: https://www.apache.org/dist/accumulo/1.8.0/SHA1SUM "1.8.0 SHA1 file hashes"
