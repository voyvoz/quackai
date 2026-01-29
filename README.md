# quackai (WIP)

An experimental DuckDB extension that enhances columns in a given table using one or multiple AI prompts via Claude Haiku. 

## Usage

Run `make` to build the extension or `make test` to build and then load the extension into DuckDB and run the aillm function.
Run `make arrow` to run a standalone executable that reads an arrow file and processes the prompts either single, fused, or batches.

Single processes the columns row by row and by prompts.
Fuse joins the prompts into a single prompt (prompt1;prompt;..) and executes row by row, but with only one request
Batch execute each column in a single batch.

A dispatcher manages prompts (no duplicates via caching) using go routines, simple error checking, and retry.

## Project Layout

Set the following system-wide:
- `QUACK_LLM_MODE=single|fused|batch`
- `ANTHROPIC_API_KEY=your-key` 

## Platform Notes

- macOS (`PLATFORM=osx_arm64` or `osx_amd64`):

  - Uses `-Wl,-undefined,dynamic_lookup` so DuckDB resolves API symbols at load time.
  - Build: `make` (auto-detects host), or `PLATFORM=osx_arm64 make`.

- Linux (`PLATFORM=linux_arm64` or `linux_amd64`):

  - Go build tag `duckdb_use_lib` links against `libduckdb.so`; the host DuckDB must match.
  - Ensure `libduckdb.so` is on the default search path or set `DUCKDB_LIB_PATH=/path/to/libduckdb.so`.

- Windows:
  - Not supported here because Windows requires all DuckDB C API symbols to be resolved at link time; the current build is designed around runtime symbol resolution (macOS) or linking to `libduckdb.so` (Linux).
