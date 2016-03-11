---
title: Bylaws
nav: nav_bylaws
---

This is version 3 of the bylaws. Community work actively continues on the bylaws, and so key segments of them are subject to change.

# Introduction

This document defines the bylaws under which the Apache Accumulo project operates. It defines the roles and responsibilities of the project, who may vote, how voting works, how conflicts are resolved, etc.

Accumulo is a project of the [Apache Software Foundation](http://www.apache.org/foundation/). The foundation holds the copyright on Apache code including the code in the Accumulo codebase. The [foundation FAQ](http://www.apache.org/foundation/faq.html) explains the operation and background of the foundation.

Accumulo is typical of Apache projects in that it operates under a set of principles, known collectively as the Apache Way. If you are new to Apache development, please refer to the [Incubator project](http://incubator.apache.org/) for more information on how Apache projects operate. Terms used at the ASF are defined in the [ASF glossary](http://www.apache.org/foundation/glossary.html).

# Roles and Responsibilities

Apache projects define a set of roles with associated rights and responsibilities. These roles govern what tasks an individual may perform within the project. The roles are defined in the following sections.

## Users

The most important participants in the project are people who use our software. The majority of our contributors start out as users and guide their development efforts from the user's perspective.

Users contribute to the Apache projects by providing feedback to contributors in the form of bug reports and feature suggestions. As well, users participate in the Apache community by helping other users on mailing lists and user support forums.

## Contributors

All of the volunteers who are contributing time, code, documentation, or resources to the Accumulo project are considered contributors. A contributor that makes sustained, welcome contributions to the project may be invited to become a committer, though the exact timing of such invitations depends on many factors.

## Committers

The project's committers are responsible for the project's technical management. Committers have write access to the project's code repositories and may cast binding votes on any technical discussion regarding Accumulo. Committer access is by invitation only and must be approved by consensus approval of the active PMC members. Upon acceptance of the invitation to become a committer, it is the accepting member’s responsibility to update their status on the Accumulo web page accordingly.

A committer is considered emeritus, meaning inactive, by their own declaration or by not reviewing patches or committing patches to the project for over six months. Emeritus members will be recognized by the PMC on the Accumulo web page, in honor of their past contributions. Emeritus members retain all voting and commit rights associated with their former designation and can move themselves out of emeritus status by sending an announcement of their return to the developer mailing list. It will be the returning member's responsibility to update their status on the web page accordingly.

An emeritus committer’s commit access may be disabled as part of routine security. Access shall not be removed without notifying the committer, and access shall be maintained if the committer wishes to leave it active. A committer’s commit access shall be reactivated upon the committer’s request to the PMC.

All Apache committers are required to have a signed [Contributor License Agreement](http://www.apache.org/licenses/icla.txt) (CLA) on file with the Apache Software Foundation. Under the terms of the CLA that all committers must sign, a committer's primary responsibility is to ensure that all code committed to Apache Accumulo is licensed appropriately and meets those criteria set forth in the CLA (including both original works and patches committed on behalf of other contributors). There is a [Committer FAQ](http://www.apache.org/dev/committers.html) which provides more details on the requirements for committers. 

It is the custom of the Accumulo project to also invite each committer to become a member of the Accumulo PMC.

## Project Management Committee

The role of the PMC, from a Foundation perspective, is [oversight](http://apache.org/foundation/how-it-works.html#pmc). The main
role of the PMC is not code and not coding, but to ensure that all legal
issues are addressed, that procedure is followed, and that each and every
release is the product of the community as a whole. That is key to our
litigation protection mechanisms.

Secondly, the role of the PMC is to further the long-term development and
health of the community as a whole, and to ensure that balanced and wide
scale peer review and collaboration does happen. Within the ASF, we worry
about any community which centers around a few individuals who are working
virtually uncontested. We believe that this is detrimental to quality,
stability, and robustness of both code and long term social structures.

The responsibilities of the PMC include:

* Deciding what is distributed as products of the Apache Accumulo project.
* Maintaining the project's shared resources, including the code repository, mailing lists, and websites.
* Protecting and ensuring proper use of Apache trademarks by the project and by other parties.
* Speaking on behalf of the project.
* Nominating new PMC members and committers.
* Maintaining these bylaws and other guidelines of the project.

In particular, PMC members must understand both our project's criteria and ASF criteria for voting on a [release](http://www.apache.org/dev/release.html#management).
See the [PMC Guide](http://www.apache.org/dev/pmc.html) for more information on PMC responsibilities.

Membership of the PMC is by invitation only and must be approved by a consensus approval of active PMC members. Upon acceptance of the invitation to become a PMC member, it is the accepting member’s responsibility to update their status on the Accumulo web page accordingly.

A PMC member is considered emeritus, meaning inactive, by their own declaration or by not contributing in any form to the project for over six months. Emeritus members will be recognized by the PMC on the Accumulo web page, in honor of their past contributions. Emeritus members retain all voting and commit rights associated with their former designation and can move themselves out of emeritus status by sending an announcement of their return to the developer mailing list. It will be the returning member's responsibility to update their status on the web page accordingly.

The chair of the PMC is appointed by the ASF board. The chair is an office holder of the Apache Software Foundation (Vice President, Apache Accumulo) and has primary responsibility to the board for the management of the projects within the scope of the Accumulo PMC. The chair reports to the board quarterly on developments within the Accumulo project.

When the current chair of the PMC resigns, the PMC votes to recommend a new chair using consensus approval, but the decision must be ratified by the Apache board.

## Release Manager

The [ASF release process](https://www.apache.org/dev/release-publishing.html) defines the [release manager](https://www.apache.org/dev/release-publishing.html#release_manager) as an individual responsible for shepherding a new project release. Any committer may serve as a release manager. The release manager for a release is chosen as part of the release plan.

At a minimum, a release manager is responsible for packaging a release candidate for a vote and signing and publishing an approved release candidate. An Accumulo release manager is also expected to:

* guide whether changes after feature freeze or code freeze should be included in the release
* ensure that required release testing is being conducted
* track whether the release is on target for its expected release date
* adjust release plan dates to reflect the latest estimates
* determine if a re-plan may be needed and, if so, call a vote
* call votes on release candidates

[Release guidelines](http://accumulo.apache.org/governance/releasing.html) and [details on the mechanics of creating an Accumulo release](http://accumulo.apache.org/releasing.html) are available on the Accumulo project site.

# Decision Making

Within the Accumulo project, different types of decisions require different forms of approval. For example, the previous section describes several decisions which require 'consensus approval'. This section defines how voting is performed, the types of approvals, and which types of decision require which type of approval.

## Voting

Decisions regarding the project are made by votes on the primary project development mailing list: dev@accumulo.apache.org. Where necessary, PMC voting may take place on the private Accumulo PMC mailing list: private@accumulo.apache.org. Votes are clearly indicated by a subject line starting with [VOTE]. A vote message may only pertain to a single item’s approval; multiple items should be separated into multiple messages. Voting is carried out by replying to the vote mail. A vote may take on one of four forms, defined below.

<table class="table">
<tr><th>Vote</th>
    <th>Meaning</th>
<tr><td>+1</td>
    <td>'Yes,' 'Agree,' or 'The action should be performed.' In general, this vote also indicates a willingness on the behalf of the voter to 'make it happen'.</td>
<tr><td>+0</td>
    <td>This vote indicates a willingness for the action under consideration to go ahead. The voter, however, will not be able to help.</td>
<tr><td>-0</td>
    <td>This vote indicates that the voter does not, in general, agree with the proposed action but is not concerned enough to prevent the action going ahead.</td>
<tr><td>-1</td>
    <td>'No', 'Disagree', or 'The action should not be performed.' On issues where consensus is required, this vote counts as a veto. All vetoes must contain an explanation of why the veto is appropriate. Vetoes with no explanation are void. It may also be appropriate for a -1 vote to include an alternative course of action.</td>
</table>

All participants in the Accumulo project are encouraged to vote. For technical decisions, only the votes of active committers are binding. Non-binding votes are still useful for those with binding votes to understand the perception of an action across the wider Accumulo community. For PMC decisions, only the votes of active PMC members are binding.

See the [voting page](http://accumulo.apache.org/governance/voting.html) for more details on the mechanics of voting.

<a name="CTR"></a>
## Commit Then Review (CTR)

Voting can also be applied to changes to the Accumulo codebase. Under the Commit Then Review policy, committers can make changes to the codebase without seeking approval beforehand, and the changes are assumed to be approved unless an objection is raised. Only if an objection is raised must a vote take place on the code change.

For some code changes, committers may wish to get feedback from the community before making the change. It is acceptable for a committer to seek approval before making a change if they so desire.

## Approvals

These are the types of approvals that can be sought. Different actions require different types of approvals.

<table class="table">
<tr><th>Approval Type</th>
    <th>Definition</th>
<tr><td>Consensus Approval</td>
    <td>A consensus approval vote passes with 3 binding +1 votes and no binding vetoes.</td>
<tr><td>Majority Approval</td>
    <td>A majority approval vote passes with 3 binding +1 votes and more binding +1 votes than -1 votes.</td>
<tr><td>Lazy Approval (or Lazy Consensus)</td>
    <td>An action with lazy approval is implicitly allowed unless a -1 vote is received, at which time, depending on the type of action, either majority approval or consensus approval must be obtained.  Lazy Approval can be either <em>stated</em> or <em>assumed</em>, as detailed on the <a href="/governance/lazyConsensus.html">lazy consensus page</a>.</td>
</table>

## Vetoes

A valid, binding veto cannot be overruled. If a veto is cast, it must be accompanied by a valid reason explaining the veto. The validity of a veto, if challenged, can be confirmed by anyone who has a binding vote. This does not necessarily signify agreement with the veto, but merely that the veto is valid.

If you disagree with a valid veto, you must lobby the person casting the veto to withdraw their veto. If a veto is not withdrawn, the action that has been vetoed must be reversed in a timely manner.

## Actions

This section describes the various actions which are undertaken within the project, the corresponding approval required for that action and those who have binding votes over the action. It also specifies the minimum length of time that a vote must remain open, measured in days. In general, votes should not be called at times when it is known that interested members of the project will be unavailable.

For Code Change actions, a committer may choose to employ assumed or stated Lazy Approval under the [CTR](#CTR) policy. Assumed Lazy Approval has no minimum length of time before the change can be made.

<table class="table">
<tr><th>Action</th>
    <th>Description</th>
    <th>Approval</th>
    <th>Binding Votes</th>
    <th>Min. Length (days)</th>
<tr><td>Code Change</td>
    <td>A change made to a codebase of the project. This includes source code, documentation, website content, etc.</td>
    <td>Lazy approval, moving to consensus approval upon veto</td>
    <td>Active committers</td>
    <td>1</td>
<tr><td>Release Plan</td>
    <td>Defines the timetable and actions for an upcoming release. The plan also nominates a Release Manager.</td>
    <td>Lazy approval, moving to majority approval upon veto</td>
    <td>Active committers</td>
    <td>3</td>
<tr><td>Release Plan Cancellation</td>
    <td>Cancels an active release plan, due to a need to re-plan (e.g., discovery of a major issue).</td>
    <td>Majority approval</td>
    <td>Active committers</td>
    <td>3</td>
<tr><td>Product Release</td>
    <td>Accepts or rejects a release candidate as an official release of the project.</td>
    <td>Majority approval</td>
    <td>Active PMC members</td>
    <td>3</td>
<tr><td>Adoption of New Codebase</td>
    <td>When the codebase for an existing, released product is to be replaced with an alternative codebase. If such a vote fails to gain approval, the existing code base will continue. This also covers the creation of new sub-projects within the project.</td>
    <td>Consensus approval</td>
    <td>Active PMC members</td>
    <td>7</td>
<tr><td>New Committer</td>
    <td>When a new committer is proposed for the project.</td>
    <td>Consensus approval</td>
    <td>Active PMC members</td>
    <td>3</td>
<tr><td>New PMC Member</td>
    <td>When a committer is proposed for the PMC.</td>
    <td>Consensus approval</td>
    <td>Active PMC members</td>
    <td>3</td>
<tr><td>New PMC Chair</td>
    <td>When a new PMC chair is chosen to succeed an outgoing chair.</td>
    <td>Consensus approval</td>
    <td>Active PMC members</td>
    <td>3</td>
<tr><td>Modifying Bylaws</td>
    <td>Modifying this document.</td>
    <td>Consensus approval</td>
    <td>Active PMC members</td>
    <td>7</td>
</table>

No other voting actions are defined; all other actions should presume Lazy Approval (defaulting to Consensus Approval upon veto). If an action is voted on multiple times, or if a different approval type is desired, these bylaws should be amended to include the action.

For the purposes of the "Adoption of New Codebase" action, the Accumulo codebase is defined as the Accumulo site content, primary project code, and all contributed code ("contribs") as they exist in their respective repositories. Adoption of a new codebase generally refers to the creation of a new contrib repository, but could cover, for example, a rework of the project site, or merging a contrib project into the primary codebase.

Voting actions for the removal of a committer or PMC member are intentionally not defined. According to ASF rules, [committer status never expires](http://www.apache.org/dev/committers.html#committer-set-term) and [PMC members can only be removed with approval from the ASF Board](http://www.apache.org/dev/pmc.html#pmc-removal).

# Release Plans

The approval of a release plan begins the process of creating a new project release. The process ends when a release candidate is approved.

An Accumulo release plan consists of at least the following:

* a version number
* a feature freeze date
* a code freeze date
* a release date
* the choice of a release manager

After feature freeze, new features should not be accepted for the release. After code freeze, only critical fixes should be accepted for the release. The release manager guides the decision on accepting changes.

All dates in a plan are estimates, as unforeseen issues may require delays. The release manager may adjust dates as needed. In serious circumstances, the release manager may opt to call a re-plan vote.

