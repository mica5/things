#!/usr/bin/env python
"""

Version 0.1
2018-07-24
"""
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import datetime
import subprocess
import re
import argparse

from sqlalchemy import DDL, func
from sqlalchemy.orm import sessionmaker

from things_config import engine, schema_name
from models import Base, SABase, Person, Kind, Thing


views = [
    '{schema}.recommendations'.format(schema=schema_name)
]

def create_tables():
    engine.execute(DDL('CREATE SCHEMA IF NOT EXISTS {schema}'.format(
        schema=schema_name,
    )))
    SABase.metadata.create_all()
    engine.execute(DDL(('''
    DROP VIEW IF EXISTS {schema}.recommendations;
    '''+'''CREATE OR REPLACE VIEW {schema}.recommendations AS
        SELECT
                t.name
                , p.name AS recommended_by
                , t.notes
                , t.location
                , k.kind
                , t.created_at AS created
            FROM things t
            INNER JOIN people p using(pid)
            INNER JOIN kind k using(kid);
    ;
    ''').format(
        schema=schema_name,
    )))

def drop_tables():
    for view in views:
        engine.execute(DDL(
            '''DROP VIEW IF EXISTS {view}'''.format(
                view=view
            )
        ))

    SABase.metadata.drop_all()


def run_main():
    args = parse_cl_args()

    if args.create_tables:
        create_tables()
    elif args.drop_tables:
        drop_tables()
    else:
        print('no action specified')

    success = True
    return success

def parse_cl_args():
    argParser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )

    argParser.add_argument(
        '--create-tables',
        default=False,
        action='store_true',
    )
    argParser.add_argument(
        '--drop-tables',
        default=False,
        action='store_true',
    )

    args = argParser.parse_args()
    return args

if __name__ == '__main__':
    success = run_main()
    exit_code = 0 if success else 1
    exit(exit_code)
