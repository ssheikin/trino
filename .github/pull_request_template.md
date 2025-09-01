# Description

Placeholder for additional information for the reviewers. 

# Fork Admission

This PR:
- [ ] Includes cherry picks from upstream (Trino OS)
- [ ] Introduces or modifies a proprietary feature: «link if exists to one of [Fork Log Items](https://github.com/starburstdata/trino-fork-log/issues)»

<!-- Mark the appropriate option with an (x). Propose a release note if you can. You may consider using AI to generate them for you. 
More info at https://trino.io/development/process#release-note -->
## Release notes

( ) This is not user-visible or is docs only, and no release notes are required.
( ) Release notes are required. Please describe the visible user experience feature/change/improvement/fix including configuration details if required. 

```markdown
## Section
* When using X the user is now able to Y ({issue}`issuenumber`)
```
---@#$---

NEW TEMPLATE TEST

## Notable Change Checklist (MUST BE FILLED BEFORE PR IS MERGED)

### Please explicitly mark each statement below as either **True** or **False** by removing unnecessary value.

- PR contains a Notable Change: **True / False**

Link to Notable Change definition: << LINK >>

### If above is "True" then fill the following:

- Link to a PR which describes this Change, if a single Change is broken into multiple PRs: **LINK** or **NONE** if not applicable
- Breaking change: **True / False** 
- Type: **config | api | sql | system**
- Severity: **critical | major | minor**
- Component: **(e.g. engine, connector, UI, docs, security, etc. — see [Component List](https://github.com/starburstdata/trino-fork-log#components))**
- Release note: **Provide release note here** 
- Jira Ticket: **e.g. SEP-12345**
- Description: **Human readable description**
- Migration Required: **True / False**

If migration is required then provide input for Migration_Guide:
- Migration_Guide: **Human readable description**

---
