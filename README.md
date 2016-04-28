# Apache Accumulo Website

Apache Accumulo uses [Jekyll](http://jekyllrb.com/) to build their website. It is recommended
that you use [Bundler](http://bundler.io/) to manage any dependencies of the build for you.

## Install Bundler and dependencies

`gem install bundler`
`bundle install`

## Build the website

Jekyll lets you either build the static HTML files or create an embedded webserver
to interact with the local version of the website.

`bundle exec jekyll build`

or

`bundle exec jekyll serve -w`

## Update the production website

For Apache Accumulo committers, the `asf-site` branch needs to be updated with the generated
HTML.

This can be done easily by invoking the post-commit hook (either by hand, or automatically via configuring
Git to invoke the post-commit hook).

`./_devtools/git-hooks/post-commit`
