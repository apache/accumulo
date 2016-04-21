---
title: Review Board Guidelines
skiph1fortitle: true
nav: nav_rb
---

# Using Review Board

The Apache Software Foundation provides an [instance][rbinstance] of
[Review Board][rb] (RB) for projects to use in code reviews. Here is how RB can
be used to support development in the context of the
[Apache Accumulo bylaws][bylaws].

Contributors to Accumulo are encouraged to use RB for non-trivial patches and
any time feedback is desired. No one is required to use RB, but its ready
availability and better interface for feedback can help with reviews. Committers
seeking review prior to pushing changes can also benefit similarly.

## Roles for Review Board

### Optional Pre-Commit Review

Accumulo operates under the [Commit-Then-Review][ctr] (CtR) policy, so code
review does not need to occur prior to commit. However, a committer may opt to
hold a code review before commit for changes that, in their opinion, would
benefit from additional attention. RB can be used to conduct such code reviews.

### Consensus Approval after a Code Change Veto

Code changes are approved by lazy approval, with consensus approval following
a veto (see the [Actions][actions] section of the bylaws). RB can be used
to coordinate a consensus approval vote by providing a convenient view of the
code change under consideration. The vote itself must still be held on the
developer mailing list.

## Guidelines for Posting a Review

* It is best to post a git-generated patch as the basis for a review. RB does
  not support patches containing multiple commits, so either squash commits
  first, or submit a diff spanning the changeset. The `rbt` and `post-review`
  tools generate diffs.
* Strive to make your changes small. Reviewers are less likely to want to
  trudge through a review with lots of changes, and are more likely to miss
  problems.
* Begin the summary of the review with a JIRA issue number. A review must
  always be associated with a JIRA issue. The issue number should also be
  entered in the "Bugs" field of the review, providing a link to JIRA.
* The "Branch" field should contain the name of the branch that the code change
  is based on (e.g., the base of your git feature branch).
* Include the "accumulo" user group as reviewers. Also include specific people
  as individual reviewers, even if they are in the "accumulo" group, if you
  deem them of primary importance for the review (e.g., you have been working
  out problems with the review with them, they are expert in the area).
* Use the description to summarize the change under review. Include helpful
  instructions to the reviewers here.
* Describe any testing done on the change. It is not expected that reviewers
  will do their own testing, and they may reject the review if you have not
  done sufficient testing yourself.
* Avoid submitting generated code for review if it can be reproduced by a
  reviewer.

After the review is published, set the status of the associated JIRA issue to
"Patch Available" as a signal to reviewers. Also, link to the review from the
issue (More -> Link -> Web Link) to help viewers of the issue find the review
and assess its progress.

## Guidelines for Reviewing Code

* Try to associate comments with relevant lines of code in the review.
* By default, each comment opens a review issue. If a comment pertains to
  something that should not block the review from completing successfully, then
  clear the "Open an issue" checkbox before saving your feedback. Examples that
  might qualify as non-issues: minor formatting/whitespace issues, spelling
  mistakes, general background questions.
* If a review looks good, be sure to "Ship It" by either using the "Ship It!"
  button or by submitting a general review with the "Ship It" checkbox checked.

## Guidelines for Completing a Review

These guidelines do not apply to consensus approval votes, since the vote
completion depends on the votes registered in the developer mailing list.

* Use your judgement for the number of "Ship It"s you want to receive to
  consider a review passed. Usually, getting one "Ship It" is enough to proceed
  with a commit. It is recommended that the "Ship It" be from a committer who
  is a neutral party not involved in the change under review.
* Use your judgement for the minimum time length you set for the review. Simple
  changes can be up for just a day, while complex ones should take up to seven.
* Review time should be extended in the face of problems discovered in the
  review. Update the review with improved code instead of discarding (i.e.,
  closing unsuccessfully) it and beginning a new one.
* A review should not be "submitted" (i.e., closed successfully) unless there
  are no outstanding issues. Check with reviewers to ensure that their issues
  are resolved satisfactorily.
* A review should be "discarded" if the code requires major rework or it
  becomes obvious it should never be committed (due to design changes,
  becoming overcome by events, being back-burnered, etc.).
* Don't leave a review open indefinitely. Once you have received sufficient
  feedback to submit or discard it, do so. If there has been no activity for
  some time, discard the review. A new one can always be created later.

Once you've closed a review as submitted, if you are unable to commit your
changes yourself, attach the final version of them to the relevant JIRA issue.
They should be in the form of a patch containing a single commit,
[per the final steps of the contribution process][contributor].

[rbinstance]: https://reviews.apache.org/
[rb]: http://www.reviewboard.org/
[bylaws]: http://accumulo.apache.org/bylaws
[ctr]: http://www.apache.org/foundation/glossary#CommitThenReview
[actions]: http://accumulo.apache.org/bylaws#actions
[contributor]: http://accumulo.apache.org/git#contributors
