# Changelog

## v0.0.7 (2022-02-11)

Updated `fractopo` to `v.0.3.0` for `tracerepo`.

### Fixes

-   (tracerepo): Refactored column code to handle `tuples` which
    `fractopo` now uses for validated trace data.

Full set of changes:
[`v0.0.6...v0.0.7`](https://github.com/nialov/tracerepo/compare/v0.0.6...v0.0.7)

## v0.0.6 (2022-01-31)

Full set of changes:
[`v0.0.5...v0.0.6`](https://github.com/nialov/tracerepo/compare/v0.0.5...v0.0.6)

## v0.0.5 (2021-12-31)

### Fixes

-   (repo): fix csv writing

-   (cli): fix validation result reporting

Full set of changes:
[`v0.0.4...v0.0.5`](https://github.com/nialov/tracerepo/compare/v0.0.4...v0.0.5)

## v0.0.4 (2021-11-11)

### New Features

-   (cli): add validity reporting in initial table

-   (cli): add nialog logging

### Fixes

-   implement support for py 3.7

-   (cli): report validation targets with table

Full set of changes:
[`v0.0.3...v0.0.4`](https://github.com/nialov/tracerepo/compare/v0.0.3...v0.0.4)

## v0.0.3 (2021-09-20)

### Fixes

-   typecheck fixes

Full set of changes:
[`v0.0.2...v0.0.3`](https://github.com/nialov/tracerepo/compare/v0.0.2...v0.0.3)

## v0.0.2 (2021-09-18)

Full set of changes:
[`v0.0.1...v0.0.2`](https://github.com/nialov/tracerepo/compare/v0.0.1...v0.0.2)

## v0.0.1 (2021-09-18)

### New Features

-   (cli): do not require command execution in cwd

-   (trace_schema): add check for lineament ids

-   validate using json config

-   implement new validation error unfit

-   fix and improve pandera report

-   add new checks for columns

-   start checking more trace dataset columns

-   add logging level settings

-   check for dangling files in data dir

-   implement format cli command

-   print export folder

### Fixes

-   (cli): continue flexible cmd execution folder

-   (cli): implement lineament id metadata

-   (cli): improve validate report

-   (trace_schema): remove geometry checks

-   (cli): tuple default argument

-   (cli): replace list defaults with tuple

-   (rules): fix filename

-   require that path points to file and exists

-   fix pre-commit issues

-   move pandera validation and handle better

-   handle pandera exceptions more widely

-   fix time string

-   handle writing errors

-   validation now occurs after parallel validate

-   add back tracevalidate cli entrypoint

-   pylint indicated bug fixes and general changes

-   rm maxworkers static value

-   try to fix filterers

-   hopefully fix verbosity

-   handle empty geodataframes

-   validate now checks empty always aswell

-   handle empty GeoDataFrames and better critical err report

-   allow validation error column coercing

-   allow validation error column to be null

-   make sure tmp dir exists

-   rename data to tracerepository_data
