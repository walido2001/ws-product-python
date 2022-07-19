import os
import json

from flask import Flask, jsonify, render_template
import sqlalchemy as sa

# web app
app = Flask(__name__)

# database engine
SQL_URI = sa.engine.URL.create(
    drivername='postgresql',
    username=os.getenv('PGUSER', 'readonly'),
    password=os.getenv('PGPASSWORD', 'w2UIO@#bg532!'),
    host=os.getenv('PGHOST', 'work-samples-db.cx4wctygygyq.us-east-1.rds.amazonaws.com'),
    database=os.getenv('PGDATABASE', 'work_samples'),
    port=os.getenv('PGPORT', 5432),
)
engine = sa.create_engine(SQL_URI)


@app.route('/')
def index():
    return 'Welcome to EQ Works 😎'


@app.route('/events/hourly')
def events_hourly():
    return query_helper('''
        SELECT date, hour, events
        FROM public.hourly_events
        ORDER BY date, hour
        LIMIT 168;
    ''')

@app.route('/events/hourly/dataTable')
def events_hourly_dataTable():
    data = query_helper_modified('''
        SELECT date, hour, events
        FROM public.hourly_events
        ORDER BY date, hour
        LIMIT 168;
    ''')
    keys = [key for key in data[0].keys()]
    print(keys)

    return render_template("dataTable.html", Title="Hourly Events DataTable", keys=keys, rows=data)

@app.route('/events/daily')
def events_daily():
    return query_helper('''
        SELECT date, SUM(events) AS events
        FROM public.hourly_events
        GROUP BY date
        ORDER BY date
        LIMIT 7;
    ''')

@app.route('/events/daily/dataTable')
def events_daily_dataTable():
    data = query_helper_modified('''
        SELECT date, SUM(events) AS events
        FROM public.hourly_events
        GROUP BY date
        ORDER BY date
        LIMIT 7;
    ''')
    keys = [key for key in data[0].keys()]
    print(keys)

    return render_template("dataTable.html", Title="Daily Events DataTable", keys=keys, rows=data)

@app.route('/stats/hourly')
def stats_hourly():
    return query_helper('''
        SELECT date, hour, impressions, clicks, revenue
        FROM public.hourly_stats
        ORDER BY date, hour
        LIMIT 168;
    ''')

@app.route('/stats/hourly/dataTable')
def stats_hourly_dataTable():
    data = query_helper_modified('''
        SELECT date, hour, impressions, clicks, revenue
        FROM public.hourly_stats
        ORDER BY date, hour
        LIMIT 168;
    ''')
    keys = [key for key in data[0].keys()]
    print(keys)

    return render_template("dataTable.html", Title="Hourly Stats DataTable", keys=keys, rows=data)

@app.route('/stats/daily')
def stats_daily():
    return query_helper('''
        SELECT date,
            SUM(impressions) AS impressions,
            SUM(clicks) AS clicks,
            SUM(revenue) AS revenue
        FROM public.hourly_stats
        GROUP BY date
        ORDER BY date
        LIMIT 7;
    ''')

@app.route('/stats/daily/dataTable')
def stats_daily_dataTable():
    data = query_helper_modified('''
        SELECT date,
            SUM(impressions) AS impressions,
            SUM(clicks) AS clicks,
            SUM(revenue) AS revenue
        FROM public.hourly_stats
        GROUP BY date
        ORDER BY date
        LIMIT 7;
    ''')
    keys = [key for key in data[0].keys()]
    print(keys)

    return render_template("dataTable.html", Title="Daily Stats DataTable", keys=keys, rows=data)

@app.route('/poi')
def poi():
    return query_helper('''
        SELECT *
        FROM public.poi;
    ''')


def query_helper(query):
    with engine.connect() as conn:
        result = conn.execute(query).fetchall()
        return jsonify([dict(row.items()) for row in result])

def query_helper_modified(query):
    with engine.connect() as conn:
        result = conn.execute(query).fetchall()
        return [dict(row.items()) for row in result]
