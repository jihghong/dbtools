from dbtools import DB
from dataclasses import dataclass
import datetime

print('---- create table')

@dataclass
class A:
    a: int = None
    b: str = None
    c: float = None

db = DB(':memory:')
table_a = db.table(A).create(unique='a')
table_aa = db.table(A, name='AA').create(unique=('a', 'b'))
table_aaa = db.table(A, name='AAA').create()     # without any unique
table_aaaa = db.table(A, name='AAAA').create(unique=['a', ('b', 'c')])
print(db.table(A).exists())          # True
print(table_a.exists())              # True
print(db.table('AA').exists())       # True
print(table_aa.exists())             # True
print(db.table('nosuch').exists())   # False
for sql, in db.execute("SELECT sql FROM sqlite_master WHERE type = 'table' ORDER BY name"): print(sql)

print('---- put')

table_a.put(A(3, 'good'))  # c is None
table_a.put(A(2, 'better', 3.14))
table_a.put(A(1, 'best', 1.414))
table_a.put(A(1, 'replaced'))
for a in table_a.all(): print(a)

print("---- put unique=['a', ('b', 'c')]")

table_aaaa.put(A(1, 'x', 2))
table_aaaa.put(A(9, 'good', 2))
try: table_aaaa.put(A(1, 'good', 2))
except Exception as e: print(repr(e))
for a in table_aaaa.all(): print(a)

print('--- count')

a = A(1, 'good', 3.14)
for i in range(3): table_aaa.put(a)
print(table_aaa.count())

print('---- orderby')

for a in table_a.orderby(a='DESC').all(): print(a)
print(table_a.orderby(b='DESC').get())

print('---- where')

for a in table_a.where(b="LIKE '%e%'").all(): print(a)

print("---- table='AA' different primary key")

table_aa.put(A(1, 'good'))
table_aa.put(A(2, 'better'))
table_aa.put(A(1, 'best'))
table_aa.put(A(2, 'better', 3.14))
table_aa.put(A(3, 'good', 1.414))
for a in table_aa.all(): print(a)

print('---- bind B')

@dataclass
class B:
    a: int = ...
    b: str = ...
    c: float = ...

view_b = table_aa.bind(B)
for b in view_b.all(): print(b)

print('---- query by example')

print(table_a.where(b="LIKE 'b%'").get())
print(table_aa.where(b='nosuch').get())  # None
print(table_a.where(A(a=..., b='replaced', c=...)).get())
for a in table_a.where(A(a=1, b=..., c=...)).all(): print(a)
for b in table_a.where(B(a=1)).all(): print(b)
print(table_aa.where(A(a=1, b=..., c=...)).get())
print(table_a.where(B(a=1)).orderby(a='DESC').get())
print(table_aa.where(B(a=1)).where(b='best').get())

print('---- update by example')

table_a.where(a=2).set(A(a=4, b='even better'))       # NOTE: this will SET c = null
table_a.where(a=3).set(A(..., 'the best', ...))       # use ... for unchanged fields
table_a.where(B(a=1)).set(B(b='modified'))
for a in table_a.all(): print(a)

print('---- bind C')

@dataclass
class C:
    a: int = 0
    b: str = ''
    # missing c
    d: str = 'from C'

view_c = table_aa.bind(C)
for c in view_c.all(): print(c)

print('---- supported types')

@dataclass
class D:
    i: int
    f: float
    s: str
    b: bytes
    d: datetime.date = ...
    t: datetime.datetime = ...

table_d = db.table(D).create()
for sql, in db.execute("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'D'"): print(sql)
table_d.put(D(4, 3.14, 'good', 'good'.encode(), datetime.date(2025, 9, 13), datetime.datetime(2025, 9, 13, 22, 15, 30, 123456)))
d = table_d.get()
print(d, type(d.i), type(d.f), type(d.s), type(d.b), type(d.d), type(d.t))
