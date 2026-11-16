# docs/ — rules

**Nothing normative lives outside `../spec/`.** A design decision is recorded
in `spec/`, or, when it is large, in an ADR under `adr/`.

**Design research, implementation plans and review handoffs are not
committed** (`research/` and `superpowers/`, which `.gitignore` excludes).
They are working files: a plan's code ends up in the code, and a review's
findings in the code, the tests or `spec/`. The rationale worth keeping goes
into `spec/` or an ADR; how the work went belongs in the pull request.
