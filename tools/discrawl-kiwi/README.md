# discrawl-kiwi

`discrawl-kiwi` is Discrawl's optional Korean lexical analyzer helper. It uses
the existing [`github.com/codingpot/kiwigo`](https://pkg.go.dev/github.com/codingpot/kiwigo)
Go binding and Kiwi's public C API. It does not use Python.

## Native prerequisites

- Kiwi 0.23.2 headers and dynamic library
- Kiwi 0.23.2 base model
- a C/C++ toolchain supported by CGO

The `kiwigo` build currently looks for headers and libraries under
`/usr/local/include` and `/usr/local/lib`. The official Kiwi release assets are:

- `kiwi_<platform>_<architecture>_v0.23.2.tgz`
- `kiwi_model_v0.23.2_base.tgz`

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
{"ready":true,"version":"0.23.2"}
{"text":"오늘 저녁먹음 기록"}
{"tokens":"오늘 저녁 먹 음 기록"}
```

## License boundary

Kiwi and `github.com/codingpot/kiwigo` are licensed under
LGPL-2.1-or-later. Discrawl invokes this separately distributed helper as an
optional process, and the helper dynamically links to the replaceable Kiwi
library. Distributors of the helper or Kiwi binary assets must include the
applicable LGPL notices and corresponding Kiwi source access.
