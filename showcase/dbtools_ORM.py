from dbtools import ORM
from dataclasses import dataclass
import datetime

print('---- create table')

@dataclass
class A:
    a: int = None
    b: str = None
    c: float = None

db = ORM(':memory:')
db.create(A, unique='a')
db.create(A, table='AA', unique=('a', 'b'))
db.create(A, table='AAA')    # without any unique
db.create(A, table='AAAA', unique=['a', ('b', 'c')])
print(db.exists(A))          # True
print(db.exists('AA'))       # True
print(db.exists('nosuch'))   # False
for sql, in db.execute("SELECT sql FROM sqlite_master WHERE type = 'table' ORDER BY name"): print(sql)

print('---- put')

db.put(A(3, 'good'))  # c is None
db.put(A(2, 'better', 3.14))
db.put(A(1, 'best', 1.414))
db.put(A(1, 'replaced'))
for a in db.all(A): print(a)

print("---- put unique=['a', ('b', 'c')]")

db.put(A(1, 'x', 2), table='AAAA')
db.put(A(9, 'good', 2), table='AAAA')
try: db.put(A(1, 'good', 2), table='AAAA')
except Exception as e: print(repr(e))
for a in db.all('AAAA'): print(a)

print('--- count')

a = A(1, 'good', 3.14)
for i in range(3): db.put(a, table='AAA')
print(db.count('AAA'))

print('---- orderby')

for a in db.all(A, orderby='a DESC'): print(a)
print(db.get(A, orderby='b DESC'))

print('---- where')

for a in db.all(A, where="b LIKE '%e%'"): print(a)

print("---- table='AA' different primary key")

db.put(A(1, 'good'), table='AA')
db.put(A(2, 'better'), table='AA')
db.put(A(1, 'best'), table='AA')
db.put(A(2, 'better', 3.14), table='AA')
db.put(A(3, 'good', 1.414), table='AA')
for a in db.all('AA'): print(a)

print('---- bind B')

@dataclass
class B:
    a: int = ...
    b: str = ...
    c: float = ...

db.bind('AA', B)
for b in db.all('AA'): print(b)

print('---- query by example')

print(db.get(A, where="b LIKE 'b%'"))
print(db.get(A, table='AA', where="b='nosuch'"))  # None
print(db.get(A(a=..., b='replaced', c=...)))
for a in db.all(A(a=1, b=..., c=...)): print(a)
for b in db.all(B(a=1), table='A'): print(b)
print(db.get(A(a=1, b=..., c=...), table='AA'))
print(db.get(B(a=1), table='A', orderby='a DESC'))
print(db.get(B(a=1), table='AA', where="b='best'"))

print('---- update by example')

db.set(A(a=4, b='even better'), where="a=2")       # NOTE: this will SET c = null
db.set(A(..., 'the best', ...), where="a=3")       # use ... for unchanged fields
db.set(B(b='modified'), table='A', where=B(a=1))
for a in db.all(A): print(a)

print('---- bind C')

@dataclass
class C:
    a: int = 0
    b: str = ''
    # missing c
    d: str = 'from C'

db.bind('AA', C)
for c in db.all('AA'): print(c)

print('---- supported types')

@dataclass
class D:
    i: int
    f: float
    s: str
    b: bytes
    d: datetime.date = ...
    t: datetime.datetime = ...

db.create(D)
for sql, in db.execute("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'D'"): print(sql)
db.put(D(4, 3.14, 'good', 'good'.encode(), datetime.date(2025, 9, 13), datetime.datetime(2025, 9, 13, 22, 15, 30, 123456)))
d = db.get(D)
print(d, type(d.i), type(d.f), type(d.s), type(d.b), type(d.d), type(d.t))
