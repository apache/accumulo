---
name: Post Vote Checklist
about: A checklist for tracking post-vote release tasks
title: 'Post-vote release checklist for version [e.g. 2.1.0]'
labels:
assignees: ''

---

- [ ] Label this issue with the [project](https://github.com/apache/accumulo/projects) that corresponds to this release version
- [Git](https://github.com/apache/accumulo) tasks
  - [ ] Create a signed `rel/<version>` tag (and push it)
  - [ ] Merge `<version>-rc<N>-next` branch into a maintenance branch (if maintenance is expected),
        and then into the `main` branch (and push them)
  - [ ] Remove `*-rc*` branches
- [Nexus](https://repository.apache.org) tasks
  - [ ] Release the staging repository corresponding to the successful release candidate (use "release" button)
  - [ ] Drop any other staging repositories for Accumulo (do *not* "release" or "promote" them)
- [SVN / dist-release](https://dist.apache.org/repos/dist/release/accumulo) tasks
  - [ ] Upload the release artifacts (tarballs, signatures, and `.sha512` files) for the mirrors
  - [ ] Remove old artifacts from the mirrors (**after** updating the website)
- [Board reporting tool](https://reporter.apache.org/addrelease?accumulo)
  - [ ] Add the date of release (the date the release artifacts were uploaded to SVN `UTC+0000`)
- Verify published artifacts
  - [ ] In [Maven Central](https://repo1.maven.org/maven2/org/apache/accumulo/accumulo-core/)
  - [ ] In [ASF Downloads](https://downloads.apache.org/accumulo)
  - [ ] In [several mirrors](https://www.apache.org/dyn/closer.lua/accumulo)
- Update the [staging website](http://accumulo.staged.apache.org/)
  - [ ] Release notes
  - [ ] Add `LTM: true` to the release notes front-matter for LTM releases
  - [ ] Navigation
  - [ ] Downloads page
  - [ ] DOAP file
  - [ ] Add manual/examples/javadoc (build javadoc from tag with `mvn clean package -DskipTests javadoc:aggregate -Paggregate-javadocs`)
  - [ ] If javadoc is built with Java 11, [patch it](https://github.com/apache/accumulo/blob/main/contrib/javadoc11.patch)
  - [ ] Jekyll config
  - [ ] Grep for, and update any links to previous version to now point to the new version
  - [ ] Update any older release notes front-matter to indicate they are either `archived: true` or `archived_critical: true`
  - [ ] [Publish to production](https://github.com/apache/accumulo-website#publishing-staging-to-production)
- Announcement email
  - [ ] Prepare and get review on dev list (see examples [from previous announcement messages](https://lists.apache.org/list.html?announce@apache.org:gte=1d:accumulo))
  - [ ] Send to announce@apache.org and user@accumulo.apache.org
- GitHub wrap-up
  - [ ] Close this issue
  - [ ] Create a new "Automated Kanban" [project](https://github.com/apache/accumulo/projects) for the next version (if necessary) and move any open issues not completed in this release to that project
  - [ ] Close the project that corresponds to this release
- Twitter
  - [ ] [Tweet it](https://tweetdeck.twitter.com)
  - [ ] [Confirm the tweet](https://twitter.com/ApacheAccumulo)

