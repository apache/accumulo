---
title: Voting
nav: nav_voting
---

Occasionally a "feel" for consensus is not enough. Sometimes we need to have a
measurable consensus. For example, when voting in new committers or to approve a
release. 

## Preparing for a Vote

Before calling a vote it is important to ensure that the community is given time to
discuss the upcoming vote. This will be done by posting an email to the list
indicating the intention to call a vote and the options available. By the time a
vote is called there should already be [consensus in the community][1]. The vote 
itself is, normally, a formality.

## Calling a Vote

Once it is time to call the vote a mail is posted with a subject starting with
"[VOTE]". This enables the community members to ensure they do not miss an important
vote thread. It also indicates that this is not consensus building but a formal
vote. The initiator is responsible for the vote. That means also to count the votes
and present the results. Everyone has 1 vote.

### Casting Your Vote

The notation used in voting is:

+1 (means I vote positive)
   You can say why you vote positive but it's not a must-have.

 0 (means I have no strong opinion, aka abstention)

-1 (means I vote negative because of the following reason)
   Yes, you must support your objection and provide an alternative course of action
   that you are willing and able to implement (where appropriate).

#### Example for a vote mail:

    Address: private@
    Subject: [VOTE] John Doe should become a regular committer
    
    Text:
    "I would like to propose to vote in John Doe as committer. John has showed in
    the last months that he has the skills and oversight for improving things (think
    about the last UI change of the "Find" dialog)."
    
    +1 (means I vote for John)
     0 (means I'm not for John but also not against to vote him in)
    -1 (means I'm not for John because of the following reason(s):
    
    Voting time frame is finished 72 hours from now until June 30, 12:00 PM UTC.

#### Example for a reply mail:

    Text:
    +1
    
    I like his work and want him to stay and to go on with his good improvements.


#### Example for a result mail:

    Subject: [VOTE][RESULTS] John Doe should become a regular committer
    
    Text:
    Vote started Thu, Jun 27, 2011 at 12:00 PM UTC, voting is now closed.
    
    Voting results:
    
    --- Numbers ---
    
    +1: 6
     0: 0
    -1: 0
    
    --- Details ---
    
    +1 John
    +1 Jane
    +1 David
    +1 Dolores
    +1 Carl
    +1 Chris

[See here for more information][2] <br>
[See here for more mail templates][3]


[1]: consensusBuilding
[2]: https://www.apache.org/foundation/voting
[3]: https://community.apache.org/newcommitter
