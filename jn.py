from contextlib import contextmanager
import datetime
from dateutil import parser
import subprocess
import sys
import os
things_dir = os.path.dirname(os.path.abspath(__file__))
if things_dir not in sys.path:
    sys.path.insert(0, things_dir)

from sqlalchemy.orm import sessionmaker
import pandas as pd
from IPython.display import display
from ipywidgets import widgets, interact_manual
import psycopg2
from ipywidgets import HTML

from things_config import engine
from models import Thing, Base

Session = sessionmaker(bind=engine)
Base.set_sess(Session)
sess = Session()

def parse_timestr(timestr):
    timestr = subprocess.check_output(
        'date -d"{}"'.format(timestr), shell=True
    ).decode().strip()
    return parser.parse(timestr)

columns = [
    'tid',
    'name',
    'recommended_by',
    'notes',
    'location',
    'kind',
    'created',
]

# make db connection
@contextmanager
def get_conn():
    conn = psycopg2.connect('postgres://localhost/mica')
    try:
        yield conn
    except KeyboardInterrupt:
        raise
    finally:
        # free up all locks
        conn.rollback()
        conn.close()

all_fields = 't.name p.name t.notes t.location_str k.kind'.split()
all_fields = ["coalesce({}, '')".format(f) for f in all_fields]
all_fields = "||' '||".join(all_fields)
def run_search(search_term):
    search_terms = search_term.strip().split()

    # if the search term is empty, return empty
    if not search_terms:
        return pd.DataFrame(columns=columns).set_index('name')

    # write the query
    ands = list()
    params = dict()
    for i, search_term in enumerate(search_terms):
        search_term = search_term.replace('\\b', '\\y')

        key = 'and{}'.format(i)
        params[key] = search_term

        ands.append("regexp_replace(all_fields, %({key})s, '', 'i')<>all_fields".format(key=key))

    query = """WITH step1 as (
        SELECT
                t.tid
                , t.name
                , p.name AS recommended_by
                , t.notes
                , t.location_str AS location
                , k.kind
                , t.created_at AS created
                , 'update things set notes='''||replace(t.notes, '''', '''''')||''' where tid='|| t.tid ||';' AS update
                , 'delete from things where tid='|| t.tid ||' ;' AS delete
                , {all_fields} AS all_fields
            FROM things t
            LEFT OUTER JOIN people p using(pid)
            LEFT OUTER JOIN kind k using(kid)
        )
        SELECT *
            FROM step1
            WHERE
            {ands}
    """.format(
        all_fields=all_fields,
        ands=('\n        AND ').join(ands),
    )

    # query the db
    with get_conn() as conn:
        df = pd.read_sql(query, conn, params=params)
    if len(df) == 0:
        return pd.DataFrame(columns=columns).set_index('tid')

    df = df.sort_values('name', ascending=False).set_index('tid')

    # df['update'] = df.apply(lambda e: "update events set description=e'{description}' where eid={eid};".format(
    #     description=e.description.replace("'", "\\'"),
    #     eid=e.eid,
    # ), axis=1)

    # return the result df
    return df

def display_recommendation_logger():

    def create_get_values():
        error_html = HTML()
        display(error_html)

        clear_error__button = widgets.Button(description='Clear error')
        clear_error__button.layout.display = 'none'
        display(clear_error__button)

        def clear_error(_):
            error_html.value = ''
            clear_error__button.layout.display = 'none'
        clear_error__button.on_click(clear_error)

        def set_error(error_msg):
            error_html.value = '<font color=red>{}</font>'.format(
                error_msg.replace('\n', '<br>\n').replace(' ', '&nbsp'),
            )
            clear_error__button.layout.display = 'block'

        @contextmanager
        def get_values(*args):
            """Get values from ipywidgets

            On success, clear the values of the ipywidgets.
            On failure, the values of the ipywidgets will be left alone.
            """
            try:
                yield [a.value for a in args]
            except KeyboardInterrupt:
                raise
            except Exception as e:
                import traceback
                set_error('{}\n{}'.format(str(e), traceback.format_exc()))
                # don't clear the values of the widgets
                return
            for a in args:
                a.value = ''
            return
        return get_values
    get_values = create_get_values()


    # create buttons
    recommendation = widgets.Text(
        description="Recommendation",
        layout=widgets.Layout(width='100%'),
    )
    recommended_by = widgets.Text(description="By/from")
    notes = widgets.Textarea(
        description="Notes",
        layout=widgets.Layout(width='100%'),
    )
    location = widgets.Text(description="Location")
    kind = widgets.Text(description="Kind")
    url = widgets.Text(description="url")
    when = widgets.Text(description="When")

    display(recommendation)
    display(kind)
    display(recommended_by)
    display(notes)
    display(when)

    display(url)
    display(location)

    html_display_of_duplicate_recommendation = HTML()
    display(html_display_of_duplicate_recommendation)
    def log_recommendation(recommendation, recommended_by, notes, location, kind, url, when):
        with get_values(recommendation, recommended_by, notes, location, kind, url, when) as values:
            recommendation, recommended_by, notes, location, kind, url, when = values

            when = when.strip()
            if when:
                when = parse_timestr(when)

            with Base.get_session() as sess:
                thing = Thing.get_row(recommendation, recommended_by, kind, sess)
                if abs(datetime.datetime.now() - thing.modified_at).total_seconds() > 1:
                    html_display_of_duplicate_recommendation.value = str(thing)
                    #run_search(thing.name).to_html()
                    message = "an entry with this thing.name already exists"
                    raise Exception(message)

                thing.notes = notes
                thing.location_str = location
                thing.url = url

                if when:
                    thing.created_at = when

                sess.commit()
        return

    elements = [
        recommendation,
        recommended_by,
        location,
        kind,
        url,
        when,
    ]

    log_recommendation_handler = lambda x: log_recommendation(
        recommendation,
        recommended_by,
        notes,
        location,
        kind,
        url,
        when,
    )
    for e in elements:
        e.on_submit(log_recommendation_handler)

# make the handler
def display_recommendation_searcher():

    pd.options.display.max_colwidth = 0

    # display the button
    return interact_manual(lambda search: run_search(search), search='')

