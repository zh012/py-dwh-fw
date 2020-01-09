import os
from jinja2 import Environment, FileSystemLoader, Template

class TemplateNotFound(Exception):
    pass


class SqlTemplate:
    def __init__(self, path, dialect=None, extension='sql'):
        self._path = path
        self._extension = extension.strip('.')
        self._dialect = dialect

    def __getattr__(self, name: str):
        return SqlTemplate(os.path.join(self._path, name), extension=self._extension, dialect=self._dialect)

    __getitem__ = __getattr__

    def __call__(self, **context):
        try_files = [
            f'{self._path}.{self._extension}'
        ]

        if self._dialect:
            try_files.insert(0, f'{self._path}.{self._dialect}.{self._extension}')

        for fn in try_files:
            try:
                with open(fn, 'r', encoding='utf8') as fp:
                    return Template(fp.read()).render(**context)
            except FileNotFoundError:
                pass

        raise TemplateNotFound(self._path, self._extension, self._dialect)

hql = SqlTemplate(os.path.join(os.path.dirname(__file__), 'templates'), dialect='hive')
