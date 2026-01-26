# AGENTS.md — Deck Automation Guardrails (AI + Humans)

This repo automates PowerPoint deck creation (Dash UI + Python generation). The #1 priority is **behavioral stability**:
new changes must NOT break approved functionality.

If a ticket requires changing existing behavior, it must say so explicitly.

---

## Non-Negotiables (Approved Functionality)

### A) UI defaults and filters (MUST NOT CHANGE)
- All filters/controls must default to **deselected / empty**.
- Users must NOT be forced to select “every Target Level Label filter” (or equivalent) to run.
- “No selection” should produce a sensible default behavior (e.g., include all / don’t filter), consistent with current app behavior.

### B) Waterfall chart labels (MUST NOT CHANGE)
- Waterfall labels must render correctly **without** the user needing to open PowerPoint’s “Edit Data”.
- Any change that touches charts must preserve the chart label-cache refresh behavior.

---

## Principle: Minimal Diff, No Refactor Creep
- Implement the smallest change that satisfies the ticket.
- Do not rename files, reorganize folders, or refactor working code unless the ticket explicitly requests it.
- If you notice tech debt, mention it as a follow-up suggestion in the PR — do not “fix” it inline.

---

## Mandatory Rule: Fuzzy Lookup for Names/Fields/Titles
Any time the code needs to find something by a textual identifier, it MUST use a **fuzzy lookup strategy**.

This includes (not limited to):
- Excel column headers (e.g., “Target Level Label”)
- Worksheet names
- Slide names / layout titles
- Shape names
- Placeholder text tokens (e.g., `<Target Level Label>`)
- Chart series/category names

### Fuzzy lookup requirements
1. Always attempt **normalized exact** match first:
   - trim whitespace, collapse internal spaces, casefold, remove punctuation variants
2. If not found, use fuzzy match:
   - Use a similarity scorer (e.g., RapidFuzz) and a threshold (recommended >= 85)
   - Return the best match ONLY if it clears the threshold AND is not ambiguous
3. If multiple candidates are close (ambiguous):
   - Fail fast with a clear error listing top candidates and their scores
4. Log the resolution:
   - e.g., `Resolved header "Target Level Label" -> "TargetLevelLabel " (score 92)`

### No brittle equals checks
Do not write code like:
- `if header == "Target Level Label": ...`
- `df["Target Level Label"]`

Instead:
- Resolve the real column name once via fuzzy lookup, then use it.

---

## Regression Tests Are Part of the Feature
Any PR that changes behavior must add/extend tests so regressions get blocked by CI.

Minimum expectation:
- New ticket = new regression test(s)
- Bug fix = test that would have failed before

---

## Required Test Coverage for This Repo

### 1) UI default state test (protects Approved A)
- Test that on initial load, all relevant filters are empty/deselected.
- Test that running with “no Target Level Label selected” still produces output (or uses default behavior) and does not error.

### 2) Waterfall label rendering test (protects Approved B)
- Generate a deck and verify chart XML contains label settings and caches such that labels render without opening “Edit Data”.
- This should be a “golden” assertion against the produced `.pptx` contents (zip + inspect `ppt/charts/chart*.xml`).

### 3) Placeholder population tests
- Verify the text placeholders like:
  - `<Target Level Label>`, `<modelled in>`, `<metric>`
  are replaced in the correct slides and not left behind.

### 4) Slide selection / template mapping tests
- If the workflow selects pre-made template slides (e.g., Waterfall Template2/3/…),
  test that the correct template slide is used for the Nth unique Target Level Label and that slide order/style remains unchanged.

### 5) Output integrity test
- Generated `.pptx` opens as a valid zip with expected parts present.
- No missing relationships; no corrupted deck artifacts.

---

## Protected High-Risk Areas (Do Not Touch Lightly)
Changes here require extra caution + tests:
- Chart XML manipulation / label caches
- Template slide identification / duplication logic
- Placeholder replacement logic
- Excel ingestion / header detection
- Dash callbacks that compute filters / defaults

If you must touch these:
- keep the diff tight
- add regression tests
- explain risk + mitigation in PR description

---

## PR Description Requirements
Every PR must include:

### Stability statement
- What changed
- What did NOT change (explicitly mention Approved A + B if relevant)
- How you verified stability (tests)

### Checklist
- [ ] Minimal diff (no refactor creep)
- [ ] UI defaults preserved (Approved A)
- [ ] Waterfall labels render without “Edit Data” (Approved B)
- [ ] Added/updated regression tests
- [ ] All tests pass

---

## If Requirements Are Ambiguous
Do not guess. Implement the least risky interpretation and document assumptions in the PR.
