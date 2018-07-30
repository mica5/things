#!/usr/bin/env python
import datetime
from contextlib import contextmanager

from sqlalchemy import (
    BigInteger, Text, Integer, DateTime,
    ForeignKey,
    Column
)

from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.schema import MetaData
from sqlalchemy.orm import relationship
from geoalchemy2 import Geography

from things_config import engine, schema_name


def pkey(id_str, dtype=Integer):
    return Column(
        id_str,
        dtype,
        autoincrement=True,
        primary_key=True,
    )


def datetime_col(colname):
    return Column(
        colname,
        DateTime,
        nullable=False,
        default="date_trunc('second', now())::timestamp",
    )

SABase = declarative_base(
    metadata=MetaData(
        bind=engine,
        schema=schema_name,
    ),
)


class Base:
    created_at = datetime_col('created_at')
    modified_at = datetime_col('modified_at')

    def __init__(self, time=None):
        if time is None:
            time = datetime.datetime.now().replace(microsecond=0)
        self.created_at = self.modified_at = time

    @classmethod
    def get_row(cls, col, value, sess):
        query = sess.query(cls).filter(col==value)

        # if it exists, then return it
        row = query.one_or_none()
        if row is not None:
            return row

        # otherwise, create one
        row = cls()
        setattr(row, str(col).split('.')[-1], value)

        # make sure the row is entered into the db and
        # has its id field populated so its id can be
        # referenced
        sess.add(row)
        sess.commit()

        return row

    Session = None
    @classmethod
    def set_sess(cls, Session):
        cls.Session = Session

    @classmethod
    @contextmanager
    def get_session(cls):
        if cls.Session is None:
            raise Exception("session not set. must be set using Base.set_sess(sqlalchemy.orm.sessionmaker(bind=engine))")
        sess = cls.Session()
        try:
            yield sess
        except KeyboardInterrupt:
            raise
        else:
            sess.commit()
        finally:
            sess.rollback()
            sess.close()

    def __repr__(self):
        attrs = list()
        for k in self.__init__.__code__.co_varnames[1:]:
            if not hasattr(self, k):
                continue
            attrs.append('{}={}'.format(
                k, repr(getattr(self, k))
            ))
        return '{}({})'.format(
            self.__class__.__name__,
            ', '.join(attrs),
        )

    def __str__(self):
        return repr(self)


class Person(Base, SABase):
    __tablename__ = 'people'
    pid = pkey('pid')
    name = Column('name', Text, unique=True)

    @classmethod
    def get_row(cls, name, sess):
        return super(Person, cls).get_row(cls.name, name, sess)

    def __repr__(self):
        return '{}(name={})'.format(
            self.__class__.__name__,
            repr(self.name),
        )


class Kind(Base, SABase):
    __tablename__ = 'kind'
    kid = pkey('kid')
    kind = Column('kind', Text, unique=True)

    @classmethod
    def get_row(cls, kind_str, sess):
        return super(Kind, cls).get_row(cls.kind, kind_str, sess)

    def __repr__(self):
        return '{}(kind={})'.format(
            self.__class__.__name__,
            repr(self.kind),
        )


class Thing(Base, SABase):
    __tablename__ = 'things'

    # primary key - "thing id"
    tid = pkey('tid', dtype=BigInteger)

    # local columns
    name = Column('name', Text)
    notes = Column('notes', Text)
    url = Column('url', Text)
    location = Column('location', Geography())
    location_str = Column('location_str', Text)


    ### Foreign columns

    pid = Column('pid', Integer, ForeignKey('people.pid'))
    person = relationship('Person')

    kid = Column('kid', Integer, ForeignKey('kind.kid'))
    kind = relationship('Kind')

    @classmethod
    def get_row(cls, name_str, recommended_by_name_str, kind_str, sess):
        thing = super(Thing, cls).get_row(cls.name, name_str, sess)

        person = Person.get_row(recommended_by_name_str, sess)
        thing.pid = person.pid

        kind = Kind.get_row(kind_str, sess)
        thing.kid = kind.kid

        return thing

    def __init__(self, name=None, notes=None, url=None, location=None, recommended_by_name_str=None, kind_str=None, time=None):
        super(Thing, self).__init__(time=time)

        self.name = name
        self.notes = notes
        self.url = url
        # self.location = location


    def __repr__(self):
        return '{}(name={}, notes={}, url={}, location={}, recommended_by_name_str={}, kind_str={}, time={})'.format(
            self.__class__.__name__,
            *[repr(obj) for obj in [
                self.name,
                self.notes,
                self.url,
                self.location,
                self.person.name,
                self.kind.kind,
                self.created_at,
            ]]
        )
    def __str__(self):
        return repr(self)
