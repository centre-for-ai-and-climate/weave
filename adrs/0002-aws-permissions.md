# AWS permissions architectural decision record

The problem: how do we give the various users and third-party services we're using
access to our AWS resources. Specifically, the various S3 buckets we have set up.

We have four kinds of things that need access:
- CAIC engineers (with local instances of code they need to test against dev S3 buckets)
- Branch deployment instances of Dagster+
- GitHub Actions which create and clean up after those branch deployments
- A production instance of Dagster+

We want to follow [AWS' best practices](https://docs.aws.amazon.com/IAM/latest/UserGuide/best-practices.html)
but each of these has slightly different constraints and possibilities.

## Engineers
We'll use the IAM Identity Center, with a user account for each person. They are then
given permission to the various AWS accounts and can access the console through AWS'
access portal.

For local development/CLI access, they can configure SSO with the identity centre and
create profile(s) in their aws config file to use.

## GitHub Actions
We'll use GitHub's OIDC to get temporary credentials to allow the action to assume a
dedicated IAM role: https://github.com/marketplace/actions/configure-aws-credentials-action-for-github-actions#oidc

## Branch and Production deployments on Dagster+
There doesn't seem to be any easy way to get temporary credentials for the dagster+
runners, so for these we will have to use IAM users and rotate credentials manually.

We'll set up a user for branch deploys and another for production, with access to the
appropriate buckets.