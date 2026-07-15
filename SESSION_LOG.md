## [2026-06-15] — odbcuite Security Fixes and Codebase Map

Mapped the odbcuite codebase with four parallel GSD mapper agents, producing seven structured documents in `.planning/codebase/`. Resolved security concerns: made NetSuite TBA credential loading lazy in `ns_token.py`, added SQL identifier allowlist validation to all four loader functions in `ns_utils.py`, removed a duplicate import, and updated `.gitignore` to exclude `.claude/settings.local.json`.

**Main artifact:** `.planning/codebase/` (7 documents) · `ns_utils.py` `_check_ident()` validator · `ns_token.py` lazy credential loading

---
