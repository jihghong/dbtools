import sqlite3
import dataclasses
import datetime


def _is_dataclass_instance(obj):
    return dataclasses.is_dataclass(obj) and not isinstance(obj, type)


class DB:
    def __init__(self, connect_string):
        self.db = sqlite3.connect(connect_string, detect_types=sqlite3.PARSE_DECLTYPES)

    def execute(self, *args, **kwargs):
        return self.db.execute(*args, **kwargs)

    def table(self, ref, name: str = None):
        return Table(self, ref, name=name)

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
        else: self.cls = None
        self._where = where or ''
        self._orderby = orderby or ''
        self._where_obj = where_obj or []
        self._columns_cache = None

    def _clone(self, *, where=None, orderby=None, cls=None, where_obj=None):
        return Table(self.db, self.cls or self.name, name=self.name, where=where if where is not None else self._where, orderby=orderby if orderby is not None else self._orderby, cls=cls if cls is not None else self.cls, where_obj=where_obj if where_obj is not None else list(self._where_obj))

    def _columns(self):
        if self._columns_cache is None:
            self._columns_cache = [info[1] for info in self.db.execute(f"PRAGMA table_info({self.name})")]
        return self._columns_cache

    def _columns_for_class(self, cls):
        return [info[1] for info in self.db.execute(f"PRAGMA table_info({cls.__name__})")]

    def create(self, unique=None, drop=False):
        if self.cls is None: raise TypeError('create expects a dataclass reference')
        columns = ['[@@object_id@@] INTEGER PRIMARY KEY']
        for field in dataclasses.fields(self.cls): columns.append(f"{field.name} {DB.sqlite3_type_mapping(field.type)}")
        columns += DB.expand_unique(unique)
        if drop: self.db.execute(f"DROP TABLE IF EXISTS {self.name}")
        self.db.execute(f"CREATE TABLE IF NOT EXISTS {self.name} ({', '.join(columns)})")
        self._columns_cache = None
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

    def put(self, obj, retrieve=False):
        fields, values = self._decompose(obj, set(self._columns()))
        if not fields: return None
        field_names = ', '.join(fields)
        place_holders = ', '.join('?' for _ in fields)
        settings = ', '.join(f"{field} = excluded.{field}" for field in fields)
        if retrieve:
            self.db.execute(f"INSERT INTO {self.name} ({field_names}) VALUES ({place_holders}) ON CONFLICT DO UPDATE SET {settings} RETURNING {field_names}", values)
            return self.get(where='ROWID = LAST_INSERT_ROWID()')
        else:
            self.db.execute(f"INSERT INTO {self.name} ({field_names}) VALUES ({place_holders}) ON CONFLICT DO UPDATE SET {settings}", values)

    def _select_fields(self):
        columns = set(self._columns())
        if self.cls:
            return [field.name for field in dataclasses.fields(self.cls) if field.name in columns]
        else:
            return [c for c in self._columns() if c != '[@@object_id@@]']

    def _render_where(self, where=None):
        parts = []
        if self._where: parts.append(self._where)
        if where: parts.append(where)
        if parts: return ' WHERE ' + ' AND '.join(parts)
        return ''

    def _render_orderby(self, orderby=None):
        order = orderby or self._orderby
        return f" ORDER BY {order}" if order else ''

    def all(self, where: str = None, orderby: str = None):
        fields = self._select_fields()
        if not fields: return
        fieldnames = ', '.join(fields)
        where_clause = self._render_where(where)
        order_clause = self._render_orderby(orderby)
        for row in self.db.execute(f"SELECT {fieldnames} FROM {self.name}{where_clause}{order_clause}"):
            yield self.cls(**dict(zip(fields, row)))

    def get(self, where: str = None, orderby: str = None):
        for obj in self.all(where=where, orderby=orderby): return obj
        return None

    def set(self, obj, where: str = None, retrieve=False):
        fields, values = self._decompose(obj, set(self._columns()))
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
