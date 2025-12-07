dbtools
=======

`dbtools` is a lightweight SQLite helper that pairs Python dataclasses with a minimal, fluent table API. It focuses on quick prototypes and internal tooling where you want to persist dataclass instances without adopting a heavyweight framework.

## Features
- Define schemas with dataclasses; field types automatically map to SQLite column types
- Perform CRUD operations via fluent `table(...).create().put().where(...).orderby(...).all()/get()/set()/count()` or direct `db.put(obj)` / `db.get(obj_or_cls, ...)` sugar
- Reuse table handles to avoid rebuilding SQL strings for chains of operations
- Use query-by-example objects or raw SQL snippets for `WHERE` and `ORDER BY`
- Bind multiple dataclasses to the same table to view the data from different angles
- Persist nested dataclass references and list fields via an automatic `@@m2m_relations@@` table (objects hydrate on fetch; unused children are refcount-deleted)
- Single-file implementation with zero third-party dependencies

## Installation

```bash
pip install git+https://github.com/jihghong/dbtools
```

## Quick Start

```python
from dataclasses import dataclass
from dbtools import DB

@dataclass
class Account:
    account_id: int = ...
    name: str = ''
    balance: float = 0.0

db = DB(':memory:')
table = db.table(Account).create(unique='account_id')

# Insert or upsert
table.put(Account(1, 'Alice', 42.0))
print(table.get(Account(account_id=1)))

# Chain filters and ordering; ... skips fields when updating
table.where(Account(account_id=1)).set(Account(..., balance=100.0))
for account in table.orderby(balance='DESC').all():
    print(account)
```

See `showcase/dbtools_DB_sugar.py` for the direct `db.put`/`db.get` style, `showcase/dbtools_DB.py` for the fluent table walkthrough (unique keys, `bind`, flexible `where`), and `showcase/dbtools_DB_many.py` for nested objects, many-to-many lists, and refcounted deletes.
