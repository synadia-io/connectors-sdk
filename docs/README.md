# docs/ — rules

**Nothing normative lives outside `../spec/`.** A design decision is recorded
in `spec/`. A decision too large for a spec section goes in an ADR under
`docs/adr/` (none yet).

**Working files are not committed.** Design research, implementation plans
and review handoffs are working files: a plan's code ends up in the code,
and a review's findings in the code, the tests or `spec/`. The rationale
worth keeping goes into `spec/` or an ADR; how the work went belongs in the
pull request. `.gitignore` excludes `docs/research/` and `docs/superpowers/`
so such files can live there without being committed.
