from flask import Flask
from flask import render_template
from flask import request
from flask_bootstrap import Bootstrap

# from invoke import run

import sys
sys.path.insert(0, '/Users/graeme/Documents/coop/actual_coops/segal/GHTorrent'
                '/mysql/connections')
sys.path.insert(0, '/Users/graeme/Documents/coop/actual_coops/segal/GHTorrent'
                '/mysql/mining_scripts')
import use_ghtorrent as gh
import probe2 as p2


def create_app():
    app = Flask(__name__)
    Bootstrap(app)
    return app

app = create_app()


@app.route('/')
def home():
    return render_template('home.html')


@app.route('/results/', methods=['GET', 'POST'])
def results():
    user = get_arg('user')
    rID = get_arg('rID')
    artID = get_arg('artID')
    time_count = get_arg('tc')
    time_measure = get_arg('tm')
    rows = get_arg('rows')
    data = {'user': user, 'rID': rID, 'artID': artID, 'time_count': time_count,
            'time_measure': time_measure, 'rows': rows}
    # return '{}'.format(", ".join(data))
    # result = run('python3 probe_data.py --p2 {time_count} {time_measure}"\
    #             " {user} {rID} {artID} {rows}'.format(**data))
    # return result.stdout
    links = main(data)
    return ", ".join(links)


def get_arg(arg_name):
    return request.args.get(arg_name)


def get_probe_2(table_name='filtered_links_dated'):
    """Create SQL query to select repo_id, linked_repo_id, and created_at from
    table_name."""

    raw_probe = "SELECT repo_id, linked_repo_id, created_at FROM {0}"
    return raw_probe.format(table_name)


def use_probe_2(docopt_args, cursor):
    import datetime
    origin_date = datetime.datetime.now()
    time_count = int(docopt_args['time_count'])
    time_measure = docopt_args['time_measure']
    submitter = docopt_args['user']
    r_ID = int(docopt_args['rID'])
    artifact_ID = int(docopt_args['artID'])
    the_links = p2.Problem2_5(origin_date, time_count, time_measure, submitter,
                              r_ID, artifact_ID)
    if docopt_args['rows']:
        row_count = int(docopt_args['rows'])
        the_links.set_row_count(row_count)
    the_links.set_urls(cursor)
    return the_links


def main(docopt_args):
    """Calculate an analyzed probe 1

        --INPUT: none
        --OUTPUT: a dict of point biserial correlations, medians, and
                    dates above the median
    """

    db_connection = gh.connect_to_ghtor()
    assert db_connection.is_connected(), 'database not connected'
    cursor = db_connection.cursor()

    answer = use_probe_2(docopt_args, cursor)
    return(answer.urls)


if __name__ == '__main__':
    app.run(debug=True)
