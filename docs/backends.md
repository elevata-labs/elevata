# Backend Prerequisites

elevata supports multiple backends for building metadata-driven data platforms.  
Each backend has specific prerequisites you need to set up before using elevata.

---

## DuckDB

- Install DuckDB via package managers (e.g. `brew install duckdb` on macOS, `apt-get install duckdb` on Linux)  
- Or download binaries: [https://duckdb.org/docs/installation](https://duckdb.org/docs/installation)  

Verify your installation:  

```bash
duckdb --version
```
Python dependencies:
```bash
pip install -r requirements/duckdb.txt
```
---

*Last updated: October 2025*