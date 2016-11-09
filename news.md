---
title: News Archive
redirect_from: /blog/
---

<div>
{% for post in site.posts %}
  {% assign post_year = post.date | date: "%Y" %}
  {% assign newer_post_year = post.next.date | date: "%Y" %}
  {% if post_year != newer_post_year %}
    <hr>
    <h3>{{ post_year }}</h3>
  {% endif %}
  <div class="row" style="margin-top: 15px">
    <div class="col-md-1">{{ post.date | date: "%b %d" }}</div>
    <div class="col-md-10"><a href="{{ site.baseurl }}{{ post.url }}">{{ post.title }}</a></div>
  </div>
{% endfor %}
</div>
