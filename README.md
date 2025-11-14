dbtools
=======

`dbtools` is a lightweight SQLite helper that pairs Python dataclasses with a minimal ORM interface. It focuses on quick prototypes and internal tooling where you want to persist dataclass instances without adopting a heavyweight framework.

## Features
- Define schemas with dataclasses; field types automatically map to SQLite column types
- Perform CRUD operations via `create`, `put`, `get`, `all`, `set`, and `count`
- Use query-by-example objects or raw SQL snippets for `WHERE` and `ORDER BY`
- Bind multiple dataclasses to the same table to view the data from different angles
- Single-file implementation with zero third-party dependencies

## Installation

```bash
pip install git+https://github.com/jihghong/dbtools
```

## Quick Start

```python
from dataclasses import dataclass
from dbtools import ORM

@dataclass
class Account:
    account_id: int = ...
    name: str = ''
    balance: float = 0.0

db = ORM(':memory:')
db.create(Account, unique='account_id')

db.put(Account(1, 'Alice', 42.0))
print(db.get(Account(account_id=1)))

# Use ... to skip fields when updating
update = Account(..., balance=100.0)
db.set(update, where=Account(account_id=1))

for account in db.all(Account, orderby='balance DESC'):
    print(account)
```

See `showcase/dbtools_ORM.py` for a more exhaustive walkthrough that covers compound unique keys, `bind`, flexible `where` clauses, and additional field types such as `datetime`.
