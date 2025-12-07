import sqlite3
import dataclasses
import datetime
from typing import get_origin, get_args


def _is_dataclass_instance(obj):
    return dataclasses.is_dataclass(obj) and not isinstance(obj, type)


def _is_dataclass_type(obj):
    return isinstance(obj, type) and dataclasses.is_dataclass(obj)


def _is_list_of_dataclass(type_):
    origin = get_origin(type_)
    if origin not in (list,):
        return False
    args = get_args(type_)
    return len(args) == 1 and _is_dataclass_type(args[0])


class ORM:
    def __init__(self, connect_string):
        self.db = sqlite3.connect(connect_string, detect_types=sqlite3.PARSE_DECLTYPES)
        self.classes = dict()

    def __getattr__(self, name):
        return getattr(self.db, name)

    def bind(self, table, cls):
        self.classes[table] = cls

    def _ensure_rel_table(self):
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS [@@m2m_relations@@] ("
            "parent_table TEXT, parent_id INTEGER, field TEXT, "
            "child_table TEXT, child_id INTEGER, "
            "PRIMARY KEY(parent_table, parent_id, field, child_table, child_id))"
        )

    @staticmethod
    def sqlite3_type_mapping(type):
        if type == int: return 'INTEGER'
        if type == float: return 'REAL'
        if type == str: return 'TEXT'
        if type == bytes: return 'BLOB'
        if type == datetime.date: return 'DATE'
        if type == datetime.datetime: return 'TIMESTAMP'
        return 'STRING'

    @staticmethod
    def expand_unique(unique):
        result = []
        if isinstance(unique, str): result.append(f"UNIQUE ({unique})")
        elif isinstance(unique, (list, tuple, set)):
            if all(isinstance(s, str) for s in unique): result.append(f"UNIQUE ({', '.join(unique)})")
            else:
                for element in unique:
                    if isinstance(element, str): result.append(f"UNIQUE ({element})")
                    elif isinstance(element, (list, tuple, set)) and all(isinstance(s, str) for s in element): result.append(f"UNIQUE ({', '.join(element)})")
                    else: raise TypeError('unique element must be a column name (str) or a list/tuple/set of column names')
        elif unique is not None: raise TypeError('unique must be a column name (str) or a list/tuple/set of column names')
        return result

    def _schema(self, cls):
        primitives, relations = [], dict()
        for field in dataclasses.fields(cls):
            ftype = field.type
            if _is_dataclass_type(ftype):
                relations[field.name] = ('object', ftype)
            elif _is_list_of_dataclass(ftype):
                relations[field.name] = ('list', get_args(ftype)[0])
            else:
                primitives.append(field)
        return primitives, relations

    def create(self, ref, table: str = None, drop=False, unique=None):
        cls = ref if isinstance(ref, type) else ref.__class__
        if table is None: table = cls.__name__
        primitives, relations = self._schema(cls)
        columns = ['[@@object_id@@] INTEGER PRIMARY KEY']
        for field in primitives: columns.append(f"{field.name} {ORM.sqlite3_type_mapping(field.type)}")
        columns += ORM.expand_unique(unique)
        if drop: self.db.execute(f"DROP TABLE IF EXISTS {table}")
        self.db.execute(f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(columns)})")
        self.classes[table] = cls
        if relations:
            self._ensure_rel_table()
            for _, (_, rel_cls) in relations.items():
                rel_table = rel_cls.__name__
                if not self.exists(rel_table): self.create(rel_cls)

    def exists(self, table):
        table = ORM.derive_table(table)
        return bool(self.db.execute("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' and name = ?", (table,)).fetchone()[0])

    def table_columns(self, table):
        return [info[1] for info in self.db.execute(f"PRAGMA table_info({table})")]

    def derive(self, ref):
        if isinstance(ref, str):
            cls, table, fields, values = self.classes.get(ref), ref, [], []
        elif isinstance(ref, type):
            cls, table, fields, values = ref, ref.__name__, [], []
        else:
            cls, table = ref.__class__, ref.__class__.__name__
            fields, values = ORM.decompose(ref, set(self.table_columns(table)))
        if cls and table not in self.classes:
            self.classes[table] = cls
        return cls, table, fields, values

    @staticmethod
    def decompose(obj, filter: set):
        fields, values = [], []
        for field in dataclasses.fields(obj.__class__):
            if field.name not in filter: continue
            value = getattr(obj, field.name)
            if value is ...: continue
            fields.append(field.name)
            values.append(value)
        return fields, values

    @staticmethod
    def zip_join(delimiter: str, fields, values):
        return delimiter.join(f"{field} = {ORM.repr(value)}" for field, value in zip(fields, values))

    @staticmethod
    def repr(value):
        if value is None: return 'null'
        else: return repr(value)

    @staticmethod
    def derive_table(table):
        if isinstance(table, type): return table.__name__
        elif isinstance(table, str): return table
        else: raise TypeError('table expects a class or table name (str)')

    def derive_where(self, where, table):
        if not where: return ''
        if isinstance(where, str): return f" WHERE {where}"
        condition = ORM.zip_join(' AND ', *ORM.decompose(where, set(self.table_columns(table))))
        if condition: return f" WHERE {condition}"
        else: return ''

    @staticmethod
    def where_condition(where: str, condition: str):
        if where:
            if condition:
                return f" WHERE {condition} AND ({where})"
            else:
                return f" WHERE {where}"
        else:
            if condition:
                return f" WHERE {condition}"
            else:
                return ''

    @staticmethod
    def _object_id(obj):
        return getattr(obj, '_object_id', None)

    @staticmethod
    def _set_object_id(obj, object_id):
        if object_id is not None:
            obj._object_id = object_id

    def _find_object_id_by_fields(self, table, obj):
        fields, values = ORM.decompose(obj, set(self.table_columns(table)))
        if not fields:
            row = self.db.execute(f"SELECT [@@object_id@@] FROM {table} ORDER BY [@@object_id@@] DESC LIMIT 1").fetchone()
            return row[0] if row else None
        condition = ' AND '.join(f"{f} = ?" for f in fields)
        row = self.db.execute(f"SELECT [@@object_id@@] FROM {table} WHERE {condition} LIMIT 1", values).fetchone()
        return row[0] if row else None

    def _get_object_id_for(self, table, obj):
        oid = self._object_id(obj)
        if oid is not None:
            return oid
        match = self._find_object_id_by_fields(table, obj)
        if match is not None:
            self._set_object_id(obj, match)
        return match

    def _insert_or_update(self, table, fields, values):
        if fields:
            field_names = ', '.join(fields)
            place_holders = ', '.join('?' for _ in fields)
            settings = ', '.join(f"{field} = excluded.{field}" for field in fields)
            self.db.execute(
                f"INSERT INTO {table} ({field_names}) VALUES ({place_holders}) "
                f"ON CONFLICT DO UPDATE SET {settings}",
                values,
            )
        else:
            self.db.execute(f"INSERT INTO {table} DEFAULT VALUES")
        row = self.db.execute(f"SELECT [@@object_id@@] FROM {table} WHERE ROWID = LAST_INSERT_ROWID()").fetchone()
        if row: return row[0]
        if fields:
            condition = ' AND '.join(f"{f} = ?" for f in fields)
            row = self.db.execute(f"SELECT [@@object_id@@] FROM {table} WHERE {condition} LIMIT 1", values).fetchone()
            return row[0] if row else None
        row = self.db.execute(f"SELECT [@@object_id@@] FROM {table} ORDER BY [@@object_id@@] DESC LIMIT 1").fetchone()
        return row[0] if row else None

    def _save_related_object(self, related_obj):
        rel_cls = related_obj.__class__
        rel_table = rel_cls.__name__
        if not self.exists(rel_table): self.create(rel_cls)
        object_id = self._get_object_id_for(rel_table, related_obj)
        if object_id is None:
            self.put(related_obj)
            object_id = self._get_object_id_for(rel_table, related_obj)
        self._set_object_id(related_obj, object_id)
        return (rel_table, object_id)

    def _extract_relations(self, obj, relations):
        rel_values = {}
        for field_name, (kind, cls) in relations.items():
            value = getattr(obj, field_name, None)
            if value is None:
                rel_values[field_name] = []
                continue
            if kind == 'object':
                rel_values[field_name] = [self._save_related_object(value)]
            elif kind == 'list':
                rel_values[field_name] = [self._save_related_object(v) for v in value]
        return rel_values

    def _replace_relations(self, table, parent_id, rel_values):
        for field_name, links in rel_values.items():
            self.db.execute(
                "DELETE FROM [@@m2m_relations@@] WHERE parent_table = ? AND parent_id = ? AND field = ?",
                (table, parent_id, field_name),
            )
            for child_table, child_id in links:
                self.db.execute(
                    "INSERT OR IGNORE INTO [@@m2m_relations@@] (parent_table, parent_id, field, child_table, child_id) VALUES (?, ?, ?, ?, ?)",
                    (table, parent_id, field_name, child_table, child_id),
                )

    def put(self, obj, table: str = None, retrieve=False):
        cls, derived_table, _, _ = self.derive(obj)
        if table is None: table = derived_table
        primitives, relations = self._schema(cls)
        columns = set(f.name for f in primitives)
        fields, values = ORM.decompose(obj, columns)
        rel_values = self._extract_relations(obj, relations)
        object_id = self._insert_or_update(table, fields, values)
        ORM._set_object_id(obj, object_id)
        if relations:
            self._ensure_rel_table()
            self._replace_relations(table, object_id, rel_values)
        if retrieve:
            return self.get(cls, table=table, where='ROWID = LAST_INSERT_ROWID()')
        else:
            return obj

    def _select_fields(self, cls, table):
        columns = set(self.table_columns(table))
        return [field.name for field in dataclasses.fields(cls) if field.name in columns]

    def _hydrate_relations(self, obj, cls, table, object_id):
        _, relations = self._schema(cls)
        if not relations: return obj
        for field_name, (kind, child_cls) in relations.items():
            rows = self.db.execute(
                "SELECT child_table, child_id FROM [@@m2m_relations@@] WHERE parent_table = ? AND parent_id = ? AND field = ? ORDER BY child_id",
                (table, object_id, field_name),
            ).fetchall()
            if not rows:
                setattr(obj, field_name, [] if kind == 'list' else None)
                continue
            if kind == 'object':
                child_obj = self._fetch_related(rows[0][0], rows[0][1])
                setattr(obj, field_name, child_obj)
            else:
                items = [self._fetch_related(t, cid) for t, cid in rows]
                setattr(obj, field_name, items)
        return obj

    def _fetch_related(self, child_table_name, child_id):
        child_cls = self.classes.get(child_table_name, None)
        if child_cls is None:
            # Try to load by name only if not bound
            child_cls = child_table_name
        return self._get_by_id(child_table_name, child_cls, child_id)

    def _get_by_id(self, table, cls, object_id):
        if isinstance(cls, str):
            columns = [c for c in self.table_columns(table) if c != '[@@object_id@@]']
            row = self.db.execute(
                f"SELECT [@@object_id@@], {', '.join(columns)} FROM {table} WHERE [@@object_id@@] = ?",
                (object_id,),
            ).fetchone()
            if not row: return None
            data = dict(zip(columns, row[1:]))
            obj = data
            ORM._set_object_id(obj, row[0])
            return obj
        fields = self._select_fields(cls, table)
        select_fields = ['[@@object_id@@]'] + fields
        row = self.db.execute(
            f"SELECT {', '.join(select_fields)} FROM {table} WHERE [@@object_id@@] = ?",
            (object_id,),
        ).fetchone()
        if not row: return None
        obj = cls(**dict(zip(fields, row[1:])))
        ORM._set_object_id(obj, row[0])
        return self._hydrate_relations(obj, cls, table, row[0])

    def select(self, cls, table: str, condition: str, where: str, orderby: str):
        fields = self._select_fields(cls, table)
        select_fields = ['[@@object_id@@]'] + fields
        where_clause = ORM.where_condition(where, condition)
        orderby_clause = f" ORDER BY {orderby}" if orderby else ''
        for row in self.db.execute(f"SELECT {', '.join(select_fields)} FROM {table}{where_clause}{orderby_clause}"):
            obj = cls(**dict(zip(fields, row[1:])))
            ORM._set_object_id(obj, row[0])
            yield self._hydrate_relations(obj, cls, table, row[0])

    def all(self, ref, table: str = None, where: str = None, orderby: str = None):
        cls, derived_table, fields, values = self.derive(ref)
        if table is None: table = derived_table
        condition = ORM.zip_join(' AND ', fields, values)
        if cls: yield from self.select(cls, table, condition, where, orderby)

    def get(self, ref, table: str = None, where: str = None, orderby: str = None):
        cls, derived_table, fields, values = self.derive(ref)
        if table is None: table = derived_table
        condition = ORM.zip_join(' AND ', fields, values)
        for obj in self.select(cls, table, condition, where, orderby): return obj  # return the first
        return None

    def set(self, obj, table: str = None, where=None, retrieve=False):
        cls = obj.__class__
        if table is None: table = cls.__name__
        primitives, _ = self._schema(cls)
        fields, values = ORM.decompose(obj, set(f.name for f in primitives))
        settings = ORM.zip_join(', ', fields, values)
        if not settings: return None
        where = self.derive_where(where, table)
        self.db.execute(f"UPDATE {table} SET {settings}{where}")
        if retrieve: return self.get(cls, table, where='ROWID = LAST_INSERT_ROWID()')

    def count(self, table, where: str = None):
        table = ORM.derive_table(table)
        where = self.derive_where(where, table)
        return self.db.execute(f"SELECT COUNT(*) FROM {table}{where}").fetchone()[0]

    def delete(self, obj=None, table: str = None, where: str = None):
        ids = []
        cls = None
        if _is_dataclass_instance(obj):
            cls, table = obj.__class__, (table or obj.__class__.__name__)
            oid = ORM._object_id(obj)
            if oid is None: oid = self._find_object_id_by_fields(table, obj)
            if oid is not None: ids.append(oid)
        elif table:
            cls, table = (table if isinstance(table, type) else self.classes.get(table)), ORM.derive_table(table)
        if where and table:
            ids.extend(r[0] for r in self.db.execute(f"SELECT [@@object_id@@] FROM {table}{self.derive_where(where, table)}").fetchall())
        for oid in ids:
            self._delete_with_relations(table, cls, oid)

    def _delete_with_relations(self, table, cls, object_id):
        rel_rows = self.db.execute(
            "SELECT field, child_table, child_id FROM [@@m2m_relations@@] WHERE parent_table = ? AND parent_id = ?",
            (table, object_id),
        ).fetchall()
        self.db.execute("DELETE FROM [@@m2m_relations@@] WHERE parent_table = ? AND parent_id = ?", (table, object_id))
        self.db.execute(f"DELETE FROM {table} WHERE [@@object_id@@] = ?", (object_id,))
        for _, child_table_name, child_id in rel_rows:
            remaining = self.db.execute(
                "SELECT COUNT(*) FROM [@@m2m_relations@@] WHERE child_table = ? AND child_id = ?",
                (child_table_name, child_id),
            ).fetchone()[0]
            if remaining == 0:
                child_cls = self.classes.get(child_table_name)
                self._delete_with_relations(child_table_name, child_cls, child_id)
