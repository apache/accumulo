---
title: Release Archive
permalink: "/release/"
redirect_from: 
  - "/release_notes/"
  - "/release_notes.html"
---

<div>
<hr>
<h3>{{ site.categories.release[0].date | date: "%Y" }}</h3>
{% for release in site.categories.release %}
  {% assign release_year = release.date | date: "%Y" %}
  {% assign newer_release_year = release.next.date | date: "%Y" %}
  {% if release_year != newer_release_year %}
    <hr>
    <h3>{{ release_year }}</h3>
  {% endif %}
  <div class="row" style="margin-top: 15px">
    <div class="col-md-1">{{ release.date | date: "%b %d" }}</div>
    <div class="col-md-10"><a href="{{ site.baseurl }}{{ release.url }}">{{ release.title }}</a></div>
  </div>
{% endfor %}
</div>
