# ScalaSparkSkillValidation
The repository to validate skill level of the dev/de

## Prerequisite:
- do not fork the original repo to your profile
- create a private repo with the same name in your profile
- add owner of the original repo to your version with `Collaborators/Add people`
- create a branch and work on a branch
- create Pull Request to master on your repository, we will review it there

## Minimal expectation:
- finish the task based on all the steps
- we will review the change and propose the changes
- the validation is not only about the code, is about the work style too
- ask any question in the `issue` on your repo or comment PR changes
- please care about well definition of the question - it's part of a validation

## Additional steps:
- please propose what can we improve in the repo/validation
- what would you change/improve in the repo right now? - add the change in the PR

## Forbidden:
- coping the work for any other repo. You can work with `internet` just do not reuse full impl.
- working with others on the task: work on you own, as we want to validate your work, not anyone else ;)

## The test - part 1:
- create spark: scala or python based project
- use Maven or Sbt
- think about the impl. as the production ready software
- prepare a HQL/SQL query to be use in `spar.sql()` API
- please use complicated logic with:
- different data types
- joins, unions, redefinition for the data set schema, fix timezone, add columns, etc.
- use 4 different data set and join them in different stage of the query
- for example, in pseudocode:
```text
table_a = (A: String, B: Timestamp, C: Date, D: Long)
table_b = (A: String, B: Timestamp, C: Date, D: Long)
table_c = (A: Long, B: Timestamp, C: Date, D: String)
table_d = (A: String, B: String, C: String, D: Long)

table_e = table_a.join(table_b, on=(C), 'left')
table_f = similar
--- etc.
table_o = table_i.union(table_j)
--- modify types: Date to String in some format
--- fix timezone
```
- prepare unit tests, that will be executed on the IDE level
- simple data: two - four rows of data per each input table
- validate results

## The test - part 2:
- the same as `part 1` but with Spark API not HQL/SQL