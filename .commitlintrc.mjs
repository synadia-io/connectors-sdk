// Conventional Commits, monorepo rules: scope = top-level SDK directory or a
// repo-wide area, closed set — a new SDK directory adds its scope here.
export default {
  extends: ['@commitlint/config-conventional'],
  rules: {
    'scope-empty': [2, 'never'],
    'scope-enum': [2, 'always', ['sdk-go', 'spec', 'conformance', 'ci', 'repo']],
  },
};
