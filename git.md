---
title: Git
nav: nav_git
---

[Git](https://git-scm.com) is an open source, distributed version control system
which has become very popular in large, complicated software projects due to
its efficient handling of multiple, simultaneously and independently developed
branches of source code.\


## Workflow Background

Likely the most contested subject matter regarding switching an active team
from one SCM tool to another is a shift in the development paradigm.

Some background, the common case, as is present with this team, is that
developers coming from a Subversion history are very light on merging being a
critical consideration on how to perform development. Because merging in
Subversion is typically of no consequence to the complete view of history, not
to mention that Subversion allows "merging" of specific revisions instead of
sub-trees. As such, a transition to Git typically requires a shift in mindset
in which a fix is not arbitrarily applied to trunk (whatever the main
development is called), but the fix should be applied to the earliest, non
end-of-life (EOL) version of the application.

For example, say there is a hypothetical application which has just released
version-3 of their software and have shifted their development to a version-4
WIP. Version-2 is still supported, while version-1 was just EOL'ed. Each
version still has a branch. A bug was just found which affects all released
versions of this application. In Subversion, considering the history in the
repository, it is of no consequence where the change is initially applied,
because a Subversion merge is capable of merging it to any other version.
History does not suffer because Subversion doesn't have the capacities to
accurately track the change across multiple branches. In Git, to maintain a
non-duplicative history, it is imperative that the developer choose the correct
branch to fix the bug in. In this case, since version-1 is EOL'ed, version-2,
version-3 and the WIP version-4 are candidates. The earliest version is
obviously version-2; so, the change should be applied in version-2, merged to
version-3 and then the version-4 WIP.

The importance of this example is making a best-attempt to preserve history
when using Git. While Git does have commands like cherry-pick which allow the
contents of a commit to be applied from one branch to another which are not
candidates to merge without conflict (which is typically the case when merging
a higher version into a lower version), this results in a duplication of that
commit in history when the two trees are eventually merged together. While Git
is smart enough to not attempt to re-apply the changes, history still contains
the blemish.

The purpose of this extravagant example is to outline, in the trivial case, how
the workflow decided upon for development is very important and has direct
impact on the efficacy of the advanced commands bundled with Git.

## Proposed Workflow

This is a summary of what has been agreed upon by vocal committers/PMC members
on [dev@accumulo.apache.org](mailto:dev@accumulo.apache.org). Enumeration of
every possible situation out of the scope of this document. This document
intends to lay the ground work to define the correct course of action regardless
of circumstances. Some concrete examples will be provided to ensure the
explanation is clear.

1. Active development is performed concurrently over no less than two versions
   of Apache Accumulo at any one point in time. As such, the workflow must
   provide guidance on how and where changes should be made which apply to
   multiple versions and how to ensure that such changes are contained in all
   applicable versions.

2. Releases are considered extremely onerous and time-consuming so no emphasis
   is placed on rapid iterations or development-cycles.

3. New features typically have lengthy development cycles in which more than
   one developer contributes to the creation of the new feature from planning,
   to implementation, to scale testing. Mechanisms/guidance should be provided
   for multiple developers to teach contribute to a remote-shared resource.

4. The repository should be the irrefutable, sole source of information
   regarding the development of Apache Accumulo, and, as such, should have
   best-efforts given in creating a clean, readable history without any single
   entity having to control all write access and changes (a proxy). In other
   words, the developers are the ones responsible for ensuring that previous
   releases are preserved to meet ASF policy, for not rewriting any public
   history of code not yet officially released (also ASF policy relevant) and
   for a best-effort to be given to avoid duplicative commits appearing in
    history created by the application of multiple revisions which have
    different identifying attributes but the same contents (git-rebase and
    git-cherry-pick).

# The implementation 

## Contributors

Use the following steps, original derived from Apache Kafka's [simple
contributor
workflow][1].

To be specific, let's consider a contributor wanting to work on a fix for the
Jira issue ACCUMULO-12345 that affects 1.5.0 release.

1. Ensure you configured Git with your information

    `git config --global user.name 'My Name' && git config --global user.email
    'myname@mydomain.com'`

2. Clone the Accumulo repository:

    `git clone https://git-wip-us.apache.org/repos/asf/accumulo.git accumulo`

3. or update your local copy:

    `git fetch && git fetch --tags`

4. For the given issue you intend to work on, choose the 'lowest' fixVersion
   and create a branch for yourself to work in. This example is against the next release of 1.5

    `git checkout -b ACCUMULO-12345-my-work origin/1.5`

5. Make commits as you see fit as you fix the issue, referencing the issue name
   in the commit message:

    `git commit -av`

    Please include the ticket number at the beginning of the log message, and
    in the first line, as it's easier to parse quickly. For example:

    `ACCUMULO-2428 throw exception when rename fails after compaction`

    Consider following the git log message format described in
    [Zach Holman's talk](https://zachholman.com/talk/more-git-and-github-secrets/)
    (specifically slides 78-98, beginning at 15:20 into the video). Essentially,
    leave a short descriptive message in the first line, skip a line, and write
    more detailed stuff there, if you need to. For example:

    `ACCUMULO-2194 Add delay for randomwalk Security teardown`

    `If two Security randomwalk tests run back-to-back, the second test may see that the
    table user still exists even though it was removed when the first test was torn down.
    This can happen if the user drop does not propagate through Zookeeper quickly enough.
    This commit adds a delay to the end of the Security test to give ZK some time.`

6. Assuming others are developing against the version you also are, as you
   work, or before you create your patch, rebase your branch against the remote
   to lift your changes to the top of your branch. The branch specified here should be the same one you used in step 4.

    `git pull --rebase origin 1.5`

7. At this point, you can create a patch file from the upstream branch to
   attach to the ACCUMULO-12345 Jira issue. The branch specified here should be teh same one you used in step 4.

    `git format-patch --stdout origin/1.5 > ACCUMULO-12345.patch`

An alternative to creating a patch is submitting a request to pull your changes
from some repository, e.g. Github. Please include the repository and branch
name merge in the notice to the Jira issue, e.g.

    repo=git://github.com/<username>/accumulo.git branch=ACCUMULO-12345

A second alternative is to use Github's "Pull Requests" feature against the
[Apache Accumulo account](https://github.com/apache/accumulo). Notifications of
new pull-requests from Github should automatically be sent to
[dev@accumulo.apache.org](mailto:dev@accumulo.apache.org).

Ignoring specifics, contributors should be sure to make their changes against
the earlier version in which the fix is intended, `git-rebase`'ing their
changes against the upstream branch so as to keep their changes co-located and
free of unnecessary merges.

## Developers

### Primary Development

Primary development should take place in `master` which is to contain the most
recent, un-released version of Apache Accumulo. Branches exist for minor releases
for each previously released major version. 

Using long-lived branches that track a major release line simplifies management and
release practices. Developers are encouraged to make branches for their own purposes,
for large features, release candidates or whatever else is deemed useful.

### Reviewing contributor changes

It is always the responsibility of committers to determine that a patch is
intended and able to be contributed.  From the
[new committer's guide](https://www.apache.org/dev/new-committers-guide#cla):
"Please take care to ensure that patches are original works which have been
clearly contributed to the ASF in public. In the case of any doubt (or when a
contribution with a more complex history is presented) please consult your
project PMC before committing it."

Extra diligence may be necessary when code is contributed via a pull request.
Committers should verify that the contributor intended to submit the code as a 
Contribution under the [Apache License](https://www.apache.org/licenses/LICENSE-2.0.txt).
When pulling the code, committers should also verify that the commits pulled match the 
list of commits sent to the Accumulo dev list in the pull request.

#### Patches

Developers should use the following steps to apply patches from
contributors:

1. Checkout the branch for the major version which the patch is intended:

    `git checkout 1.5`

2. Verify the changes introduced by the patch:

    `git apply --stat ACCUMULO-12345.patch`

3. Verify that the patch applies cleanly:

    `git apply --check ACCUMULO-12345.patch`

4. If all is well, apply the patch:

    `git am --signoff < ACCUMULO-12345.patch`

5. When finished, push the changes:

    `git push origin 1.5`

6. Merge where appropriate:

    `git checkout master && git merge 1.5`

#### Github Pull-Requests

If the contributor submits a repository and branch to pull
from, the steps are even easier:

1. Add their repository as a remote to your repository

    `git remote add some_name ${repository}`

2. Fetch the refs from the given repository

    `git fetch ${repository}`

3. Merge in the given branch to your local branch

    `git merge some_name/${branch}`

4. Delete the remote:

    `git remote remove some_name`

If the branch doesn't fast-forward merge, you likely want to inform the
contributor to update their branch to avoid the conflict resolution and merge
commit. See the [Git
manual](https://git-scm.com/book/en/Git-Branching-Basic-Branching-and-Merging)
for more information on merging. When merging a pull-request, it's best to **not**
include a signoff on the commit(s) as it changes the final commit ID in the
Accumulo repository. This also has the negative of not automatically closing
the Pull-Request when the changes are made public.

### Feature Branches

Ease in creating and using feature branches is a desirable merit which Git
provides with little work. Feature branches are a great way in which developers
to work on multiple, long-running features concurrently, without worry of
mixing code ready for public-review and code needing more internal work.
Additionally, this creates an easily consumable series of commits in which
other developers can track changes, and see how the feature itself evolved.

To prevent developers' feature branches from colliding with one another, it was
suggested to impose a "hierarchy" in which shared feature branches are prefixed
with `<apache_id>/ACCUMULO-<issue#>[-description]`.

1. Create a branch off of `master`.

    `git checkout <apache_id>/ACCUMULO-<issue#> master`

2. Create the feature, commiting early and often to appropriately outline the
"story" behind the feature and it's implementation.

3. As long as you have not collaborating with others, `git-rebase` your feature
branch against upstream changes in `master`

    `git fetch && git rebase origin/master`

4. If you are actively collaborating with others, you should be nice and not
change their history. Use `git-merge` instead.

    `git fetch && git merge origin/master`

5. Continue steps 2 through 4 until the feature is complete.

6. Depending on the nature, duration and history of the changes in your feature
branch, you can choose to:

    * **'Simple' Merge**: 

        `git checkout master && git merge <apache_id>/ACCUMULO-<issue#>`

    * **Rebase and merge** -- keeps all feature-branch commits
      co-located: 

        `git fetch && git rebase origin/master && git checkout master && git merge <apache_id>/ACCUMULO-<issue#>`

    * **Merge with squash** -- feature branch history is a mess, just make one commit
      with the lump-sum of your feature branch changes: 

        `git checkout master && git merge --squash <apache_id>/ACCUMULO-<issue#>`

### Changes which affect multiple versions (a.k.a. merging)

Merging can be a very confusing topic for users switching to Git, but it can be
summarized fairly easily.

0. **Precondition**: choose the right branch to start! (lowest, applicable version
   for the change)

1. Get your changes fixed for that earliest version.

2. Switch to the next highest version which future minor versions will be
   released (non-EOL major release series).

3. `git-merge` the branch from #1 into the current.

4. In the face of conflicts, use options from `git-merge` to help you.

    * `git checkout new-version && git merge --stat old-version`
    * `git checkout new-version && git merge --no-commit old-version`

5. Treat your current branch as the branch from #2, and repeat from #2.

When merging changes across major releases, there is always the possibility of
changes which are applicable/necessary in one release, but not in any future
releases, changes which are different in implementation due to API changes, or
any number of other cases. Whatever the actual case is, the developer who made
the first set of changes (you) is the one responsible for performing the merge
through the rest of the active versions. Even when the merge may results in a
zero-length change in content, this is incredibly important to record as you
are the one who knows that this zero-length change in content is correct!

## Release Management

Releases, although not a day to day task, have their own unique steps which are
to be followed. Releases can be categorized in to minor and major releases.

### A minor release

A minor release is some set of changes `z'` on top of a version `x.y.z`.
Typically, `z'` is simply `z + 1`, e.g. given a release named '1.6.0', and the
next minor release is '1.6.1'. These changes for `z'` should not break any
client code which works against `z` and should absolutely not change the public
API.

By convention, the branch containing the changes `z'` should be named
`x.y` (where the changes for `z'` are commits since `x.y.z`. The steps to take are as follows:

1. Prepare the release candidate. [Release
   Guide]({{ site.baseurl }}/governance/releasing), [Maven
   Instructions]({{ site.baseurl }}/releasing)
2. Create a branch for the release candidate from the `x.y` branch,
   named something like `x.y.z'-RCN`.
3. Test and Vote
4. Create a GPG-signed tag with the correct final name: `x.y.z'`
5. Push a delete of the remote branch `x.y.z'-RCN`

This process is not firm and should not be viewed as requirements for making a release.
The release manager is encouraged to make use branches/tags in whichever way is best.

### A major release

A major release is a release in which multiple new features are introduced
and/or the public API are modified. The major release `y'`, even when the
client API is not modified, will typically contain many new features and
functionality above the last release series `y`. A major release also resets
the `z` value back to `0`.

The steps to create a new major release are very similar to a minor release:

1. Prepare the release candidate. _reference release instructions_
2. Create a tag of the release candidate from the `x.y` branch,
   named something like `x.y.0-RCN`.
3. Test and Vote
4. Create a GPG-signed tag with the correct final name: `x.y.0`
5. Push a delete of the remote branch `x.y.0-RCN`

# The infrastructure

This section deals with the changes that must be requested through INFRA. As
with any substantial INFRA request, the VOTE and result from the mailing should
be referenced so INFRA knows that the request has been acknowledged. Likely, a
PMC member should be the one to submit the request.

## Repositories

I believe that we will need multiple repositories to best align ourselves with
how we currently track "Accumulo" projects. The repositories follow:

1. The main source tree. This will track the standard trunk, branches, tags
   structure from Subversion for Apache Accumulo.

2. One repository for every project in
   [contrib](https://svn.apache.org/repos/asf/accumulo/contrib): Accumulo-BSP,
   Instamo Archetype, and the Wikisearch project. Each of these
   are considered disjoint from one another, and the main source tree, so they
   each deserve their own repository.

Given the list of repositories that currently exist on the [ASF
site](https://git-wip-us.apache.org/repos/asf) and a brief search over INFRA
tickets, multiple repositories for a single Apache project is not an issue.
Having this list when making the initial ticket will likely reduce the amount
of work necessary in opening multiple INFRA tickets.

## Mirroring

It should be noted in the INFRA requst that each repository will also need to
be configured to properly mirror to the [ASF Github](https://github.com/apache)
account to provide the same functionality with current have via the git+svn
mirror. This should be noted in the INFRA request. Same change needs to be
applied for the [Apache hosted](https://git.apache.org) mirror'ing.

## Mailing lists

It should be noted in the INFRA request that commit messages should be sent to
[commits@accumulo.apache.org](mailto:commits@accumulo.apache.org). The subject
can be decided on using the [provided
variables](https://git-wip-us.apache.org/docs/switching-to-git#contents).

# Examples

For the sake of clarity, some examples of common situations are included below.

## Releasing 1.6.0

1. Branch from `master` to `1.6`

    `git checkout master && git branch 1.6`

2. Tag `1.6.0-RC1` from the just created `1.6` branch

    `git tag 1.6.0-RC1 1.6`

3. Test, vote, etc. and tag from 1.6.0-RC1

    `git -s tag 1.6.0 1.6.0-RC1`

4. Delete the RC tag, if desired.

    `git tag -d 1.6.0-RC1 && git push --delete origin 1.6.0-RC1`

5. Ensure `master` contains all features and fixes from `1.6.0`

    `git checkout master && git merge 1.6`

6. Update the project version in `master` to 1.7.0-SNAPSHOT


[1]: https://cwiki.apache.org/confluence/display/KAFKA/Patch+submission+and+review#Patchsubmissionandreview-Simplecontributorworkflow

