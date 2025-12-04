# ⚙️ Target Backend Prerequisites

elevata supports multiple backends for building metadata-driven data platforms.  
Each backend has specific prerequisites you need to set up before using elevata.

---

## 🔧 1. DuckDB

- Install DuckDB via python (`pip install duckdb`), via package managers (e.g. `brew install duckdb` on macOS, `apt-get install duckdb` on Linux)  
- Or download binaries: [https://duckdb.org/docs/installation](https://duckdb.org/docs/installation)  

Verify your installation:  

```bash
duckdb --version
```
Python dependencies for elevata:
```bash
pip install -r requirements/duckdb.txt
```

DuckDB is also used internally for SQL preview rendering.

---

© 2025 elevata Labs — Internal Technical Documentation