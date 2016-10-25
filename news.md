---
title: News Archive
redirect_from: /blog/
---

<table>
{% for post in site.posts %}
  {% assign post_year = post.date | date: "%Y" %}
  {% assign newer_post_year = post.next.date | date: "%Y" %}
  {% if post_year != newer_post_year %}
  <tr><th colspan="2">{{ post_year }}</th></tr>
  {% endif %}
  <tr><td style="padding: 10px"><small>{{ post.date | date: "%b %d" }}</small></td><td><a href="{{ site.baseurl }}{{ post.url }}" class="post-title-archive">{{ post.title }}</a></td></tr>
{% endfor %}
</table>
