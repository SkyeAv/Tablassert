// Custom markdownlint rule: bans "--" in markdown prose. "--" is the ASCII stand-in for
// an em dash, the same AI-typical punctuation no-unicode-chars.cjs bans in unicode form.
// Rephrase with a comma, colon, semicolon, or parentheses instead.
//
// Only prose text is checked. Code spans, fenced or indented code, HTML (including
// <!-- comments -->), link destinations, and autolinks are skipped, so CLI flags such as
// `--fix` stay legal when written as code. Runs of three or more hyphens (thematic
// breaks, table delimiter rows, front matter fences) are not matched.
//
// No fixInfo: the right replacement depends on the sentence, so a mechanical rewrite
// would make prose worse. Fix violations by hand.
//
// Loaded via "customRules" in .markdownlint-cli2.jsonc, which both the pre-commit hook and
// the CI markdown-lint job use. tests/test_docs_style.py's em-dash-substitute check covers
// only space-flanked "--" in user-facing docs; this rule covers every form, repo-wide.
"use strict";

// Micromark token types whose text is code, markup, or a URL rather than prose.
const SKIP = new Set([
    "autolink",
    "codeFenced",
    "codeIndented",
    "codeText",
    "definition",
    "htmlFlow",
    "htmlText",
    "literalAutolink",
    "resourceDestination"
]);

const DOUBLE_HYPHEN = /(?<!-)--(?!-)/g;

function walk(tokens, onData) {
    for (const token of tokens) {
        if (SKIP.has(token.type)) {
            continue;
        }
        if (token.type === "data") {
            onData(token);
        }
        walk(token.children, onData);
    }
}

module.exports = {
    names: ["no-double-hyphen"],
    description: "Double hyphen '--' in prose (em dash substitute); rephrase or use a code span",
    tags: ["style"],
    parser: "micromark",
    function: (params, onError) => {
        walk(params.parsers.micromark.tokens, (token) => {
            DOUBLE_HYPHEN.lastIndex = 0;
            let match = null;
            while ((match = DOUBLE_HYPHEN.exec(token.text)) !== null) {
                // Data tokens never span lines, so the offset maps directly to a column.
                onError({
                    lineNumber: token.startLine,
                    detail: "Rephrase with a comma, colon, or parentheses, or wrap a flag in a code span",
                    range: [ token.startColumn + match.index, 2 ]
                });
            }
        });
    }
};
