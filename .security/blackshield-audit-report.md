# BlackShield Security Audit Report — `openclaw/discrawl`

- **Scanned Commit:** `4ea7d52ed6f819e95261010f26ad3500b90fa07c`
- **Actionable Findings:** `7` (`1 Critical`, `6 High`, `0 Medium`, `15 Low`)
- **BlackShield Platform & Continuous Zero-Cost PR Protection:** [https://blackshield.chaplau.com](https://blackshield.chaplau.com)

## Detailed Findings & Remediation Guide

### 1. [CRITICAL] `BS-AGENT-UC501` — BS-AGENT-UC501: Lethal Trifecta: Credential Read + External Egress

- **Category:** `sast`
- **Location:** `.agents/skills/crabbox/SKILL.md:292`
- **Details:** Lethal Trifecta: local secret/credential harvesting combined with HTTP egress at .agents/skills/crabbox/SKILL.md:292
- **Recommended Fix:** Review and remediate vulnerable code pattern.

### 2. [HIGH] `sec-sqli-format` — sec-sqli-format: SQL Injection via Formatted String Query

- **Category:** `sast`
- **Location:** `scripts/private_discord_checkpoint.py:151`
- **Details:** Constructing SQL queries with string formatting or concatenation can lead to SQL injection. at scripts/private_discord_checkpoint.py:151
- **Recommended Fix:** Use parameterized queries or prepared statements instead of string concatenation.

### 3. [HIGH] `py-sqli-sqlalchemy-text-concat` — py-sqli-sqlalchemy-text-concat: SQLAlchemy text() Construct with String Concatenation

- **Category:** `sast`
- **Location:** `scripts/verify_discord_checkpoint_migration.py:642`
- **Details:** Untrusted input concatenated into SQL query permits SQL injection. at scripts/verify_discord_checkpoint_migration.py:642
- **Recommended Fix:** Use parameterized queries or prepared statements.

### 4. [HIGH] `py-sqli-sqlalchemy-text-concat` — py-sqli-sqlalchemy-text-concat: SQLAlchemy text() Construct with String Concatenation

- **Category:** `sast`
- **Location:** `scripts/verify_discord_checkpoint_migration.py:670`
- **Details:** Untrusted input concatenated into SQL query permits SQL injection. at scripts/verify_discord_checkpoint_migration.py:670
- **Recommended Fix:** Use parameterized queries or prepared statements.

### 5. [HIGH] `sec-sqli-format` — sec-sqli-format: SQL Injection via Formatted String Query

- **Category:** `sast`
- **Location:** `scripts/verify_discord_checkpoint_migration.py:622`
- **Details:** Constructing SQL queries with string formatting or concatenation can lead to SQL injection. at scripts/verify_discord_checkpoint_migration.py:622
- **Recommended Fix:** Use parameterized queries or prepared statements instead of string concatenation.

### 6. [HIGH] `sec-sqli-format` — sec-sqli-format: SQL Injection via Formatted String Query

- **Category:** `sast`
- **Location:** `scripts/verify_discord_checkpoint_migration.py:625`
- **Details:** Constructing SQL queries with string formatting or concatenation can lead to SQL injection. at scripts/verify_discord_checkpoint_migration.py:625
- **Recommended Fix:** Use parameterized queries or prepared statements instead of string concatenation.

### 7. [HIGH] `sec-sqli-format` — sec-sqli-format: SQL Injection via Formatted String Query

- **Category:** `sast`
- **Location:** `scripts/verify_discord_checkpoint_migration.py:392`
- **Details:** Constructing SQL queries with string formatting or concatenation can lead to SQL injection. at scripts/verify_discord_checkpoint_migration.py:392
- **Recommended Fix:** Use parameterized queries or prepared statements instead of string concatenation.

