import sqlite3
import dataclasses
import datetime

class ORM:
    def __init__(self, connect_string):
        self.db = sqlite3.connect(connect_string, detect_types=sqlite3.PARSE_DECLTYPES)
        self.classes = dict()

    def __getattr__(self, name):
        return getattr(self.db, name)

    def bind(self, table, cls):
        self.classes[table] = cls

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

    def create(self, ref, table: str = None, drop=False, unique=None):
        if type(ref) == type: cls = ref
        else: cls = ref.__class__
        if table is None: table = cls.__name__
        columns = ['[@@object_id@@] INTEGER PRIMARY KEY']
        for field in dataclasses.fields(cls): columns.append(f"{field.name} {ORM.sqlite3_type_mapping(field.type)}")
        columns += ORM.expand_unique(unique)
        if drop: self.db.execute(f"DROP TABLE IF EXISTS {table}")
        self.db.execute(f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(columns)})")
        self.classes[table] = cls

    def exists(self, table):
        table = ORM.derive_table(table)
        return bool(self.db.execute(f"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' and name = ?", (table,)).fetchone()[0])

    def table_columns(self, table):
        return [info[1] for info in self.db.execute(f"PRAGMA table_info({table})")]

    def derive(self, ref):
        if isinstance(ref, str):
            cls, table, fields, values = self.classes.get(ref), ref, [], []
        elif type(ref) == type:
            cls, table, fields, values = ref, ref.__name__, [], []
        else:
            cls, table = ref.__class__, ref.__class__.__name__
            fields, values = ORM.decompose(ref, set(self.table_columns(table)))
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

    def put(self, obj, table: str = None, retrieve=False):
        cls, derived_table, fields, values = self.derive(obj)
        if table is None: table = derived_table
        if not fields: return None
        field_names = ', '.join(fields)
        place_holders = ', '.join('?' for _ in fields)
        settings = ', '.join(f"{field} = excluded.{field}" for field in fields)
        if retrieve:
            self.db.execute(f"INSERT INTO {table} ({field_names}) VALUES ({place_holders}) ON CONFLICT DO UPDATE SET {settings} RETURNING {field_names}", values)
            return self.get(cls, table=table, where='ROWID = LAST_INSERT_ROWID()')
        else:
            self.db.execute(f"INSERT INTO {table} ({field_names}) VALUES ({place_holders}) ON CONFLICT DO UPDATE SET {settings}", values)

    def select(self, cls, table:str, condition: str, where: str, orderby: str):
        columns = set(r[1] for r in self.db.execute(f"PRAGMA table_info({table})"))
        fields = [field.name for field in dataclasses.fields(cls) if field.name in columns]
        if fields:
            fieldnames = ', '.join(fields)
            where = ORM.where_condition(where, condition)
            orderby = f" ORDER BY {orderby}" if orderby else ''
            for row in self.db.execute(f"SELECT {fieldnames} FROM {table}{where}{orderby}"):
                yield cls(**dict(zip(fields, row)))

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
        where = self.derive_where(where, table)
        settings = ORM.zip_join(', ', *ORM.decompose(obj, set(self.table_columns(table))))
        self.db.execute(f"UPDATE {table} SET {settings}{where}")
        if retrieve: return self.get(cls, table, where='ROWID = LAST_INSERT_ROWID()')

    def count(self, table, where: str = None):
        table = ORM.derive_table(table)
        where = self.derive_where(where, table)
        return self.db.execute(f"SELECT COUNT(*) FROM {table}{where}").fetchone()[0]

