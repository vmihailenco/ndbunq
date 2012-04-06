import hashlib
import logging

from google.appengine.ext import ndb


__all__ = ['Error', 'UniqueConstraintViolation', 'Unique', 'Model']


logger = logging.getLogger('ndbunq')


class Error(Exception):
    """Base exception for model module"""


class UniqueConstraintViolation(Error):
    """Raised when unique constraint is violated"""

    def __init__(self, values, hsh):
        self.values = values
        self.hash = hsh
        Error.__init__(self,
            'Unique constraint violated for values=%r (%s)' %
            (self.values, self.hash))


class Unique(ndb.Model):
    @classmethod
    def create(cls, value):
        entity = cls(key=ndb.Key(cls, value))
        txn = lambda: entity.put() if not entity.key.get() else None
        return ndb.transaction(txn) is not None

    @classmethod
    def create_multi(cls, values):
        keys = [ndb.Key(cls, value) for value in values]
        entities = [cls(key=k) for k in keys]
        func = lambda e: e.put() if not e.key.get() else None
        created = [ndb.transaction(lambda: func(e)) for e in entities]

        if created != keys:
            # A poor man's rollback: delete all recently created records.
            ndb.delete_multi(k for k in created if k)
            return False, [k.id() for k in keys if k not in created]

        return True, []


class MetaModel(ndb.MetaModel):
    def __new__(cls, name, bases, classdict):
        classdict['Unique'] = type(name + 'Unique', (Unique,), {})
        classdict['UniqueConstraintViolation'] = type(
            'UniqueConstraintViolation', (UniqueConstraintViolation,), {})
        new_class = super(MetaModel, cls).__new__(cls, name, bases, classdict)
        new_class._meta = new_class.Meta()
        return new_class


class Model(ndb.Model):
    __metaclass__ = MetaModel

    class Meta:
        unique = []

    @classmethod
    def _from_pb(cls, *args, **kwargs):
        ent = super(Model, cls)._from_pb(*args, **kwargs)
        ent._save_orig_values()
        return ent

    def _pre_put_hook(self):
        creation = not self._has_complete_key()
        self._check_unique_constraints(creation=creation)

    @classmethod
    def _pre_delete_hook(cls, key):
        if key is not None:
            instance = key.get()
            if not instance:
                logger.error(
                    '_pre_delete_hook: key=%r: instance not found' % key)
                return
            instance._delete_unique_values()

    def _save_orig_values(self):
        orig_values = {}
        for prop in self._properties:
            try:
                orig_values[prop] = getattr(self, prop)
            except AttributeError:
                continue
        self._orig_values = orig_values

    def _check_unique_constraints(self, creation=False):
        if creation:
            for props in self._meta.unique:
                self._check_props_uniqueness(props)
        else:
            for props in self._meta.unique:
                if not self.prop_changed(*props):
                    continue

                hsh, _ = self._props_hash(props, **{'orig': True})
                ndb.Key(self.Unique, hsh).delete()

                self._check_props_uniqueness(props)

    def _check_props_uniqueness(self, props):
        fixer = 'fix_%s_uniqueness' % '_'.join(props).lower()
        fixer = getattr(self, fixer, None)
        orig_values = {}
        if fixer:
            for prop in props:
                orig_values[prop] = getattr(self, prop)

        for i in xrange(100):
            hsh, values = self._props_hash(props)
            if self.Unique.create(hsh):
                return
            if fixer:
                fixer(i, **orig_values)
            else:
                raise self.UniqueConstraintViolation(values, hsh)

    def _delete_unique_values(self):
        for props in self._meta.unique:
            hsh, _ = self._props_hash(*props)
            ndb.Key(self.Unique, hsh).delete_async()

    def _props_hash(self, props, **kwargs):
        orig = kwargs.pop('orig', False)

        hsh = hashlib.sha1()
        values = []
        for prop in props:
            if orig:
                value = self.get_orig_value(prop)
            else:
                value = getattr(self, prop)
            if isinstance(value, unicode):
                value = str(value)
            values.append((prop, value))
        hsh.update(repr(values))
        hsh = hsh.hexdigest()
        return hsh, values
