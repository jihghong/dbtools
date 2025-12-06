from dataclasses import dataclass, field
from dbtools import DB

print("---- create 'B has A' tables")


@dataclass
class A:
    a: int = 0


@dataclass
class B:
    a: A = None
    b: str = None


db = DB(":memory:")
table_b = db.table(B).create()

print("Table A exists:", db.table(A).exists())
print("Table B exists:", table_b.exists())

for sql, in db.execute("SELECT sql FROM sqlite_master WHERE type = 'table' ORDER BY name"):
    print(sql)

print("---- put B")

table_b.put(B(A(1), "good"))
table_b.put(B(A(5), "better"))
table_b.put(B(A(10), "best"))

print("Raw contents of A (id, a):")
print(db.execute("SELECT [@@object_id@@], a FROM A ORDER BY a").fetchall())
print("Raw contents of B (id, b):")
print(db.execute("SELECT [@@object_id@@], b FROM B ORDER BY b").fetchall())
print("Raw contents of @@m2m_relations@@:")
print(db.execute("SELECT * FROM [@@m2m_relations@@] ORDER BY parent_table, parent_id, field, child_table, child_id").fetchall())

print("---- hydrate B objects")
for b in table_b.orderby("b").all():
    print(b, "-> a:", b.a)

print("\n\n---- many-to-many Book & Author ----")


@dataclass
class Author:
    name: str = None


@dataclass
class Book:
    title: str = None
    authors: list[Author] = field(default_factory=list)


books = db.table(Book).create(unique="title")
print("Table Author exists:", db.table(Author).exists())
print("Table Book exists:", books.exists())
print("Table @@m2m_relations@@ exists:", db.exists('@@m2m_relations@@'))

for sql, in db.execute("SELECT sql FROM sqlite_master WHERE type = 'table' AND name IN ('Author', 'Book', '[@@m2m_relations@@]') ORDER BY name"):
    print(sql)

print("---- put books with authors")
a1, a2, a3 = Author("Alice"), Author("Bob"), Author("Charlie")
books.put(Book("Book 1", [a1, a2]))
books.put(Book("Book 2", [a2, a3]))

print("Authors rows:")
print(db.execute("SELECT [@@object_id@@], name FROM Author ORDER BY name").fetchall())
print("Books rows:")
print(db.execute("SELECT [@@object_id@@], title FROM Book ORDER BY title").fetchall())
print("@@m2m_relations@@ rows:")
print(db.execute("SELECT * FROM [@@m2m_relations@@] WHERE parent_table = 'Book' ORDER BY parent_id, child_id").fetchall())

print("---- hydrate book with authors")
book1 = books.get(where="title = 'Book 1'")
print(book1)
print("Authors:", [a.name for a in book1.authors])

book2 = books.get(where="title = 'Book 2'")
print(book2)
print("Authors:", [a.name for a in book2.authors])

print("---- update authors (replace Bob with David)")
a4 = Author("David")
books.put(Book("Book 1", [a1, a4]))
print(db.execute("SELECT [@@object_id@@], name FROM Author ORDER BY name").fetchall())
print(db.execute("SELECT * FROM [@@m2m_relations@@] WHERE parent_table = 'Book' ORDER BY parent_id, child_id").fetchall())
book1 = books.get(where="title = 'Book 1'")
print("Book 1 authors after update:", [a.name for a in book1.authors])

print("\n\n---- mixed relation (Matchup) ----")


@dataclass
class Team:
    name: str


@dataclass
class Matchup:
    home: Team = None
    away: Team = None
    alternates: list[Team] = field(default_factory=list)


matchups = db.table(Matchup).create()
matchups.put(
    Matchup(
        home=Team("Falcons"),
        away=Team("Wolves"),
        alternates=[Team("Owls"), Team("Bulls")],
    )
)
saved_match = matchups.get()
print("home:", saved_match.home)
print("away:", saved_match.away)
print("alternates:", saved_match.alternates)

print("\n\n---- multiple references to same class ----")


@dataclass
class Assignment:
    reviewer1: Team = None
    reviewer2: Team = None
    backups: list[Team] = field(default_factory=list)


assignments = db.table(Assignment).create()
assignments.put(
    Assignment(
        reviewer1=Team("Group A"),
        reviewer2=Team("Group B"),
        backups=[Team("Group C"), Team("Group D")],
    )
)
assignment = assignments.get()
print("reviewer1:", assignment.reviewer1)
print("reviewer2:", assignment.reviewer2)
print("backups:", assignment.backups)

print("\n\n---- refcount deletion (last ref removes child) ----")


@dataclass
class Tag:
    name: str


@dataclass
class Article:
    title: str
    tags: list[Tag] = field(default_factory=list)


articles = db.table(Article).create(unique="title")
shared = Tag("shared")
articles.put(Article("First", [shared, Tag("solo1")]))
articles.put(Article("Second", [shared, Tag("solo2")]))

print("Tags before deletes:", db.execute("SELECT [@@object_id@@], name FROM Tag ORDER BY name").fetchall())

first = articles.get(where="title = 'First'")
articles.delete(first)
print("Tags after deleting First (shared should remain):", db.execute("SELECT [@@object_id@@], name FROM Tag ORDER BY name").fetchall())

second = articles.get(where="title = 'Second'")
articles.delete(second)
print("Tags after deleting Second (shared now removed):", db.execute("SELECT [@@object_id@@], name FROM Tag ORDER BY name").fetchall())

print("\nDone.")
