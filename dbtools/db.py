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


class DB:
    def __init__(self, connect_string):
        self.db = sqlite3.connect(connect_string, detect_types=sqlite3.PARSE_DECLTYPES)
        self.classes = dict()

    def execute(self, *args, **kwargs):
        return self.db.execute(*args, **kwargs)

    def table(self, ref, name: str = None):
        if isinstance(ref, type) and dataclasses.is_dataclass(ref):
            table_name = name or ref.__name__
            self.classes[table_name] = ref
        return Table(self, ref, name=name)

    def exists(self, table):
        table = DB.derive_table(table)
        return bool(self.db.execute("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' and name = ?", (table,)).fetchone()[0])

    def _ensure_rel_table(self):
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS [@@m2m_relations@@] ("
            "parent_table TEXT, parent_id INTEGER, field TEXT, "
            "child_table TEXT, child_id INTEGER, "
            "PRIMARY KEY(parent_table, parent_id, field, child_table, child_id))"
        )

    @staticmethod
    def sqlite3_type_mapping(type_):
        if type_ == int: return 'INTEGER'
        if type_ == float: return 'REAL'
        if type_ == str: return 'TEXT'
        if type_ == bytes: return 'BLOB'
        if type_ == datetime.date: return 'DATE'
        if type_ == datetime.datetime: return 'TIMESTAMP'
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

    @staticmethod
    def repr(value):
        if value is None: return 'null'
        else: return repr(value)

    @staticmethod
    def derive_table(table):
        if isinstance(table, type): return table.__name__
        elif isinstance(table, str): return table
        else: raise TypeError('table expects a class or table name (str)')


class Table:
    def __init__(self, db: DB, ref, name: str = None, where: str = None, orderby: str = None, cls=None, where_obj=None):
        self.db = db
        if name is None:
            if isinstance(ref, str): name = ref
            else: name = ref.__name__
        self.name = name
        if cls is not None: self.cls = cls
        elif isinstance(ref, type): self.cls = ref
        elif _is_dataclass_instance(ref): self.cls = ref.__class__
        else: self.cls = db.classes.get(self.name)
        if _is_dataclass_type(self.cls):
            self.db.classes[self.name] = self.cls
        self._where = where or ''
        self._orderby = orderby or ''
        self._where_obj = where_obj or []
        self._columns_cache = None
        self._schema_cache = None

    def _clone(self, *, where=None, orderby=None, cls=None, where_obj=None):
        return Table(
            self.db,
            self.cls or self.name,
            name=self.name,
            where=where if where is not None else self._where,
            orderby=orderby if orderby is not None else self._orderby,
            cls=cls if cls is not None else self.cls,
            where_obj=where_obj if where_obj is not None else list(self._where_obj),
        )

    def _columns(self):
        if self._columns_cache is None:
            self._columns_cache = [info[1] for info in self.db.execute(f"PRAGMA table_info({self.name})")]
        return self._columns_cache

    def _columns_for_class(self, cls):
        return [info[1] for info in self.db.execute(f"PRAGMA table_info({cls.__name__})")]

    def _schema(self):
        if self._schema_cache is not None: return self._schema_cache
        primitives, relations = [], dict()
        if self.cls:
            for field in dataclasses.fields(self.cls):
                ftype = field.type
                if _is_dataclass_type(ftype):
                    relations[field.name] = ('object', ftype)
                elif _is_list_of_dataclass(ftype):
                    relations[field.name] = ('list', get_args(ftype)[0])
                else:
                    primitives.append(field)
        self._schema_cache = (primitives, relations)
        return self._schema_cache

    def create(self, unique=None, drop=False):
        if self.cls is None: raise TypeError('create expects a dataclass reference')
        primitives, relations = self._schema()
        columns = ['[@@object_id@@] INTEGER PRIMARY KEY']
        for field in primitives: columns.append(f"{field.name} {DB.sqlite3_type_mapping(field.type)}")
        columns += DB.expand_unique(unique)
        if drop: self.db.execute(f"DROP TABLE IF EXISTS {self.name}")
        self.db.execute(f"CREATE TABLE IF NOT EXISTS {self.name} ({', '.join(columns)})")
        self._columns_cache = None
        if relations:
            self.db._ensure_rel_table()
            for _, (_, rel_cls) in relations.items():
                rel_table = self.db.table(rel_cls)
                if not rel_table.exists(): rel_table.create()
        return self

    def exists(self):
        table = DB.derive_table(self.name)
        return bool(self.db.execute("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' and name = ?", (table,)).fetchone()[0])

    def bind(self, cls):
        return self._clone(cls=cls)

    def where(self, obj=None, **kwargs):
        new_cls = None
        new_cond = self._condition_from(obj, kwargs)
        if _is_dataclass_instance(obj): new_cls = obj.__class__
        new_where_obj = list(self._where_obj)
        if _is_dataclass_instance(obj): new_where_obj.append(obj)
        if self._where and new_cond:
            combined = f"{self._where} AND ({new_cond})"
        elif new_cond:
            combined = new_cond
        else:
            combined = self._where
        return self._clone(where=combined, cls=new_cls, where_obj=new_where_obj)

    def orderby(self, *args, **kwargs):
        order = ''
        if args:
            order = args[0]
        elif kwargs:
            order = ', '.join(f"{k} {v}" for k, v in kwargs.items())
        return self._clone(orderby=order)

    def _condition_from(self, obj, kwargs):
        conds = []
        if obj:
            if isinstance(obj, str):
                conds.append(obj)
            elif _is_dataclass_instance(obj):
                fields, values = self._decompose(obj, set(self._columns_for_class(obj.__class__)))
                conds.extend(f"{f} = {DB.repr(v)}" for f, v in zip(fields, values))
            else:
                raise TypeError('where expects str or dataclass instance')
        for key, value in kwargs.items():
            if value is ...: continue
            if value is None:
                conds.append(f"{key} IS NULL")
            elif isinstance(value, str) and (value.strip().upper().startswith(('LIKE', 'IN', 'NOT', 'IS', 'BETWEEN')) or ' ' in value or value[0] in ('<', '>', '!', '=')):
                conds.append(f"{key} {value}")
            else:
                conds.append(f"{key} = {DB.repr(value)}")
        return ' AND '.join(conds)

    @staticmethod
    def _decompose(obj, filter_set: set):
        fields, values = [], []
        for field in dataclasses.fields(obj.__class__):
            if field.name not in filter_set: continue
            value = getattr(obj, field.name)
            if value is ...: continue
            fields.append(field.name)
            values.append(value)
        return fields, values

    @staticmethod
    def _object_id(obj):
        return getattr(obj, '_object_id', None)

    @staticmethod
    def _set_object_id(obj, object_id):
        if object_id is not None:
            obj._object_id = object_id

    def put(self, obj, retrieve=False):
        primitives, relations = self._schema()
        columns = set(f.name for f in primitives)
        fields, values = self._decompose(obj, set(self._columns()))
        rel_values = self._extract_relations(obj, relations)
        object_id = self._insert_or_update(fields, values)
        self._set_object_id(obj, object_id)
        if relations:
            self.db._ensure_rel_table()
            self._replace_relations(object_id, rel_values, relations)
        if retrieve:
            return self.get(where='ROWID = LAST_INSERT_ROWID()')
        return obj

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

    def _save_related_object(self, related_obj):
        rel_cls = related_obj.__class__
        rel_table = self.db.table(rel_cls)
        if not rel_table.exists(): rel_table.create()
        object_id = rel_table._get_object_id_for(related_obj)
        if object_id is None:
            rel_table.put(related_obj)
            object_id = rel_table._get_object_id_for(related_obj)
        self._set_object_id(related_obj, object_id)
        return (rel_table.name, object_id)

    def _get_object_id_for(self, obj):
        oid = self._object_id(obj)
        if oid is not None:
            return oid
        match = self._find_object_id_by_fields(obj)
        if match is not None:
            self._set_object_id(obj, match)
        return match

    def _find_object_id_by_fields(self, obj):
        fields, values = self._decompose(obj, set(self._columns()))
        if not fields:
            row = self.db.execute(f"SELECT [@@object_id@@] FROM {self.name} ORDER BY [@@object_id@@] DESC LIMIT 1").fetchone()
            return row[0] if row else None
        condition = ' AND '.join(f"{f} = ?" for f in fields)
        row = self.db.execute(f"SELECT [@@object_id@@] FROM {self.name} WHERE {condition} LIMIT 1", values).fetchone()
        return row[0] if row else None

    def _insert_or_update(self, fields, values):
        if fields:
            field_names = ', '.join(fields)
            place_holders = ', '.join('?' for _ in fields)
            settings = ', '.join(f"{field} = excluded.{field}" for field in fields)
            self.db.execute(
                f"INSERT INTO {self.name} ({field_names}) VALUES ({place_holders}) "
                f"ON CONFLICT DO UPDATE SET {settings}",
                values,
            )
        else:
            self.db.execute(f"INSERT INTO {self.name} DEFAULT VALUES")
        row = self.db.execute(f"SELECT [@@object_id@@] FROM {self.name} WHERE ROWID = LAST_INSERT_ROWID()").fetchone()
        if row: return row[0]
        return self._find_object_id_by_fields_placeholder(fields, values)

    def _find_object_id_by_fields_placeholder(self, fields, values):
        if not fields:
            row = self.db.execute(f"SELECT [@@object_id@@] FROM {self.name} ORDER BY [@@object_id@@] DESC LIMIT 1").fetchone()
            return row[0] if row else None
        condition = ' AND '.join(f"{f} = ?" for f in fields)
        row = self.db.execute(f"SELECT [@@object_id@@] FROM {self.name} WHERE {condition} LIMIT 1", values).fetchone()
        return row[0] if row else None

    def _replace_relations(self, parent_id, rel_values, relations):
        for field_name, links in rel_values.items():
            self.db.execute(
                "DELETE FROM [@@m2m_relations@@] WHERE parent_table = ? AND parent_id = ? AND field = ?",
                (self.name, parent_id, field_name),
            )
            for child_table, child_id in links:
                self.db.execute(
                    "INSERT OR IGNORE INTO [@@m2m_relations@@] (parent_table, parent_id, field, child_table, child_id) VALUES (?, ?, ?, ?, ?)",
                    (self.name, parent_id, field_name, child_table, child_id),
                )

    def _select_fields(self):
        columns = set(self._columns())
        fields = []
        if self.cls:
            for field in dataclasses.fields(self.cls):
                if field.name in columns:
                    fields.append(field.name)
        else:
            fields = [c for c in self._columns() if c != '[@@object_id@@]']
        return fields

    def _render_where(self, where=None):
        parts = []
        if self._where: parts.append(self._where)
        if where: parts.append(where)
        if parts: return ' WHERE ' + ' AND '.join(parts)
        return ''

    def _render_orderby(self, orderby=None):
        order = orderby or self._orderby
        return f" ORDER BY {order}" if order else ''

    def _hydrate_relations(self, obj, object_id):
        _, relations = self._schema()
        if not relations: return obj
        for field_name, (kind, child_cls) in relations.items():
            rows = self.db.execute(
                "SELECT child_table, child_id FROM [@@m2m_relations@@] WHERE parent_table = ? AND parent_id = ? AND field = ? ORDER BY child_id",
                (self.name, object_id, field_name),
            ).fetchall()
            if not rows:
                setattr(obj, field_name, [] if kind == 'list' else None)
                continue
            if kind == 'object':
                child_obj = self._fetch_related(rows[0][0], rows[0][1])
                setattr(obj, field_name, child_obj)
            else:
                items = [self._fetch_related(table, cid) for table, cid in rows]
                setattr(obj, field_name, items)
        return obj

    def _fetch_related(self, child_table_name, child_id):
        child_cls = self.db.classes.get(child_table_name, None)
        child_table = self.db.table(child_cls or child_table_name)
        return child_table._get_by_id(child_id)

    def _get_by_id(self, object_id):
        fields = self._select_fields()
        if not fields and not self.cls:
            return None
        select_fields = ['[@@object_id@@]'] + fields
        row = self.db.execute(
            f"SELECT {', '.join(select_fields)} FROM {self.name} WHERE [@@object_id@@] = ?",
            (object_id,),
        ).fetchone()
        if not row: return None
        obj = self.cls(**dict(zip(fields, row[1:]))) if self.cls else dict(zip(fields, row[1:]))
        self._set_object_id(obj, row[0])
        return self._hydrate_relations(obj, row[0])

    def all(self, where: str = None, orderby: str = None):
        fields = self._select_fields()
        if not fields and not self.cls: return
        select_fields = ['[@@object_id@@]'] + fields
        where_clause = self._render_where(where)
        order_clause = self._render_orderby(orderby)
        for row in self.db.execute(f"SELECT {', '.join(select_fields)} FROM {self.name}{where_clause}{order_clause}"):
            obj = self.cls(**dict(zip(fields, row[1:]))) if self.cls else dict(zip(fields, row[1:]))
            self._set_object_id(obj, row[0])
            yield self._hydrate_relations(obj, row[0])

    def get(self, where: str = None, orderby: str = None):
        for obj in self.all(where=where, orderby=orderby): return obj
        return None

    def set(self, obj, where: str = None, retrieve=False):
        primitives, _ = self._schema()
        columns = set(f.name for f in primitives)
        fields, values = self._decompose(obj, columns)
        settings = ', '.join(f"{field} = {DB.repr(value)}" for field, value in zip(fields, values))
        if not settings: return None
        where_clause = self._render_where(where)
        if not where_clause and self._where_obj:
            fallback = self._fallback_where_from_objects()
            if fallback: where_clause = ' WHERE ' + fallback
        self.db.execute(f"UPDATE {self.name} SET {settings}{where_clause}")
        if retrieve: return self.get(where='ROWID = LAST_INSERT_ROWID()')

    def _fallback_where_from_objects(self):
        conds = []
        for obj in self._where_obj:
            fields, values = self._decompose(obj, set(self._columns()))
            conds.extend(f"{f} = {DB.repr(v)}" for f, v in zip(fields, values))
        return ' AND '.join(conds)

    def count(self, where: str = None):
        where_clause = self._render_where(where)
        return self.db.execute(f"SELECT COUNT(*) FROM {self.name}{where_clause}").fetchone()[0]

    def delete(self, obj=None, where: str = None):
        ids = []
        if _is_dataclass_instance(obj):
            oid = self._object_id(obj)
            if oid is None:
                oid = self._find_object_id_by_fields(obj)
            if oid is not None: ids.append(oid)
        if where:
            ids.extend(r[0] for r in self.db.execute(f"SELECT [@@object_id@@] FROM {self.name}{self._render_where(where)}").fetchall())
        if not ids and obj is None and where is None:
            return
        for oid in ids:
            self._delete_with_relations(oid)

    def _delete_with_relations(self, object_id):
        rel_rows = self.db.execute(
            "SELECT field, child_table, child_id FROM [@@m2m_relations@@] WHERE parent_table = ? AND parent_id = ?",
            (self.name, object_id),
        ).fetchall()
        self.db.execute("DELETE FROM [@@m2m_relations@@] WHERE parent_table = ? AND parent_id = ?", (self.name, object_id))
        self.db.execute(f"DELETE FROM {self.name} WHERE [@@object_id@@] = ?", (object_id,))
        for _, child_table_name, child_id in rel_rows:
            remaining = self.db.execute(
                "SELECT COUNT(*) FROM [@@m2m_relations@@] WHERE child_table = ? AND child_id = ?",
                (child_table_name, child_id),
            ).fetchone()[0]
            if remaining == 0:
                child_cls = self.db.classes.get(child_table_name)
                child_table = self.db.table(child_cls or child_table_name)
                child_table.delete(where=f"[@@object_id@@] = {child_id}", obj=None)
