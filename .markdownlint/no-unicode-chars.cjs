// Custom markdownlint rule: bans AI-typical unicode punctuation that "sometimes ends up"
// in markdown files and auto-fixes it to ASCII equivalents:
//
//   em/en dashes:  — (U+2014), – (U+2013)              -> -
//   arrows:        → (U+2192), ⇒ (U+21D2), ⟶ (U+27F6), -> ->
//                  ➜ (U+279C), ➔ (U+2794), ⇨ (U+21E8)  -> ->
//   left arrow:    ← (U+2190)                          -> <-
//   bullets:       • (U+2022), ‣ (U+2023), ◦ (U+25E6)  -> -
//
// Curated blocklist, not a blanket unicode ban: legitimate notation (≤, ≥, ×, ≥, °, é, …)
// stays legal. Extend the tables below rather than loosening the rule.
//
// Loaded via "customRules" in .markdownlint-cli2.jsonc, which both the pre-commit hook and
// the CI markdown-lint job use, so the ban is enforced identically everywhere. The fixInfo
// below makes `markdownlint-cli2 --fix` rewrite violations automatically.
//
// NOTE on interplay with tests/test_docs_style.py (the repo's pre-existing typography gate):
// it bans ASCII arrows ("->", "<-") in user-facing docs PROSE (outside code spans), and its
// file list covers README/CHANGELOG/CONTRIBUTING/CODE_OF_CONDUCT/docs. A rule-fixed arrow in
// that prose will therefore turn the pytest gate red by design; the correct follow-up there
// is to rephrase or wrap the arrow in a code span. The two gates are complementary:
// markdownlint = structure + unicode ban repo-wide, pytest gate = stricter prose typography
// in user-facing files only.
"use strict";

const GROUPS = [
    {
        label: "em/en dash",
        replacement: "-",
        regex: /[\u2013\u2014]/g
    },
    {
        label: "left arrow",
        replacement: "<-",
        regex: /\u2190/g
    },
    {
        label: "arrow",
        replacement: "->",
        regex: /[\u2192\u21d2\u27f6\u279c\u2794\u21e8]/g
    },
    {
        label: "bullet",
        replacement: "-",
        regex: /[\u2022\u2023\u25e6]/g
    }
];

module.exports = {
    names: ["no-unicode-dashes-arrows-bullets", "no-unicode-chars"],
    description: "Disallowed unicode punctuation (em/en dash, arrow, bullet); use ASCII",
    tags: ["style"],
    function: (params, onError) => {
        for (const [ index, line ] of params.lines.entries()) {
            for (const { label, replacement, regex } of GROUPS) {
                regex.lastIndex = 0;
                let match = null;
                while ((match = regex.exec(line)) !== null) {
                    const column = match.index + 1;
                    const codePoint =
                        match[0].codePointAt(0).toString(16).toUpperCase().padStart(4, "0");
                    onError({
                        lineNumber: index + 1,
                        detail:
                            `Unicode ${label} '${match[0]}' (U+${codePoint})` +
                            `; use '${replacement}'`,
                        range: [ column, match[0].length ],
                        fixInfo: {
                            editColumn: column,
                            deleteCount: match[0].length,
                            insertText: replacement
                        }
                    });
                }
            }
        }
    }
};
