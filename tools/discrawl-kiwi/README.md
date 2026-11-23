# discrawl-kiwi

`discrawl-kiwi` is Discrawl's optional Korean lexical analyzer helper. It uses
the existing [`github.com/codingpot/kiwigo`](https://pkg.go.dev/github.com/codingpot/kiwigo)
Go binding and Kiwi's public C API. It does not use Python.

## Native prerequisites

- Kiwi 0.24.0 headers and dynamic library
- Kiwi 0.24.0 base model
- a C/C++ toolchain supported by CGO

The `kiwigo` build currently looks for headers and libraries under
`/usr/local/include` and `/usr/local/lib`. The official Kiwi release assets are:

- `kiwi_<platform>_<architecture>_v0.24.0.tgz`
- `kiwi_model_v0.24.0_base.tgz`

Build:

```bash
bash install-kiwi.sh
go build -o discrawl-kiwi .
```

Run:

```bash
discrawl-kiwi --model /path/to/models/cong/base
```

The helper speaks newline-delimited JSON over stdin/stdout and stays alive so
the model is loaded once:

```text
{"ready":true,"version":"0.24.0"}
{"text":"오늘 저녁먹음 기록"}
{"tokens":"오늘 저녁 먹 음 기록"}
```

## License boundary

Kiwi 0.24.0 is licensed under Apache-2.0; the unchanged
`github.com/codingpot/kiwigo` binding remains LGPL-2.1. Discrawl invokes this separately distributed helper as an
optional process, and the helper dynamically links to the replaceable Kiwi
library. Distributors of the helper or Kiwi binary assets must include the
applicable license notices and satisfy the binding's LGPL source requirements.

Query requests add `"query":true` to the JSON request. The response includes
`"groups":[["surface","base"],["required-term"]]`: groups are conjoined,
while equivalent terms within one group are alternatives. Index requests
continue to return the flat `tokens` string. Build helpers from the same
Discrawl source revision as the CLI.
