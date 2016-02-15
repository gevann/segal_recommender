from flask import Flask
from flask import render_template
from flask import request
from flask_bootstrap import Bootstrap

import sys
import datetime
sys.path.insert(0, '/Users/graeme/Documents/coop/actual_coops/segal/GHTorrent'
                '/mysql/connections')
sys.path.insert(0, '/Users/graeme/Documents/coop/actual_coops/segal/GHTorrent'
                '/mysql/mining_scripts')
import use_ghtorrent as gh
import probe2 as p2

TOOL_NAME = "Suggestinator"


def create_app():
    app = Flask(__name__)
    Bootstrap(app)
    return app

app = create_app()


@app.route('/')
def home():
    return render_template('home.html', tool_name=TOOL_NAME)


@app.route('/suggestions/', methods=['GET', 'POST'])
def results():
    user_repo = get_arg('user_repo')
    user, repo = user_repo.split("/")
    rID = get_arg('rID')
    artID = get_arg('artID')
    time_count = get_arg('tc')
    time_measure = get_arg('tm')
    rows = get_arg('rows')
    data = {'user': user, 'rID': rID, 'artID': artID, 'time_count': time_count,
            'time_measure': time_measure, 'rows': rows, 'repo': repo}
    solved_problem = main(data)
    urls = []
    for i, entry in enumerate(solved_problem.urls):
        urls.append({'url': entry,
                     'repo': solved_problem.link_data[i][0],
                     'user': solved_problem.link_data[i][1]})
    used_non_strict = solved_problem.used_non_strict
    return render_template('suggestions.html', urls=urls, used_non_strict=used_non_strict,
                           nothing_found=solved_problem.nothing_found, tool_name=TOOL_NAME)


@app.route('/integration/')
def integration():
    return render_template('integration.html', tool_name=TOOL_NAME)


def get_arg(arg_name):
    return request.args.get(arg_name)


def get_rID(repo_name, table_name='filtered_links_dated'):
    '''Query for the unique repository ID for the given repository name.'''

    inputs = (table_name, repo_name)
    qry = "SELECT repo_id FROM %s WHERE repo = %s LIMIT 1;"
    return qry, inputs


def get_probe_2(table_name='filtered_links_dated'):
    """Create SQL query to select repo_id, linked_repo_id, and created_at from
    table_name."""

    inputs = (table_name,)
    qry = "SELECT repo_id, linked_repo_id, created_at FROM %s"
    return qry, inputs


def use_probe_2(docopt_args, cursor):
    origin_date = datetime.datetime.now()
    time_count = int(docopt_args['time_count'])
    time_measure = docopt_args['time_measure']
    submitter = docopt_args['user']
    r_ID_qry, params = get_rID(docopt_args['repo'])
    cursor.execute(r_ID_qry, params)
    r_ID = int(cursor.fetchall()[0][0])
    artifact_ID = int(docopt_args['artID'])
    the_links = p2.Problem2_5(origin_date, time_count, time_measure, submitter,
                              r_ID, artifact_ID)
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
    return(answer)


if __name__ == '__main__':
    app.run(debug=True)
