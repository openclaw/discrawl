# `lexical`

Rebuild the configured multilingual search indexes from the local archive:

```bash
discrawl lexical rebuild
discrawl --json lexical rebuild
```

Configure `[search.lexical].languages` and install the optional helpers first.
See [search modes](../guides/search-modes.html) for supported languages and setup.

The command holds the archive writer lock, reads current messages, and replaces
each language index transactionally. It does not contact Discord or update a
Git snapshot. Large archives can take time to reindex.

Run it after enabling languages or replacing a helper, dictionary, native Kiwi
library, or model at the same path. Stop running writers before replacing these
files. Changes to configured helper/model paths are detected automatically on
the next writer open; read-only searches request a rebuild when their configured
analyzer does not match the index.
