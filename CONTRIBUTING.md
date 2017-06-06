# Contributors Guide

 If you believe that you have found a bug, please search for an existing [issue](https://issues.apache.org/jira/browse/accumulo) to see if it has already been reported. If you would like to add a new feature to Accumulo, please send an email with your idea to the [user](mailto:user@accumulo.apache.org) mail list. If it's appropriate, then we will create a ticket and assign it to you.

## Development

- Ensure that your work targets the correct branch
- Add / update unit and integration tests
- Ensure that Accumulo builds cleanly using the command: `mvn clean verify`

## Patch Submission

- Before submission please squash your commits using a message that contains the JIRA issue number and a description of the changes.
- Patches should be submitted in the form of Pull Requests to the Apache Accumulo GitHub [repository](https://github.com/apache/accumulo/) or to the [Review Board](https://reviews.apache.org) accumulo repository.

## Review

- One or more committers will review your patch. 
- They will be looking for things like threading issues, performance implications, API design, etc.
- Reviewers will likely ask questions to better understand your change - be prepared to answer them.
- Reviewers will make comments about changes to your patch:
    - MUST means that the change is required
    - SHOULD means that the change is suggested, further discussion on the subject may be required
    - COULD means that the change is optional

