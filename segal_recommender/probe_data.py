"""probe_data.py

    USAGE:
        probe_data.py --p1 <repo>...
        probe_data.py --p1 --mean <repo>...
        probe_data.py --p2 [--hind] [<tc> <tm> <user> <rID> <artID>] [<rows>]

"""

from docopt import docopt
import sys
import os
sys.path.insert(0, '/Users/graeme/Documents/coop/actual_coops/segal/GHTorrent'
                '/mysql/connections')
sys.path.insert(0, '/Users/graeme/Documents/coop/actual_coops/segal/GHTorrent'
                '/mysql/mining_scripts')
import average_issues_by_week as by_week
import use_ghtorrent as gh
import pickle
from scipy.stats import pointbiserialr
import probe2 as p2
import numpy


def get_probe_1(table_name):
    """Create SQL query to select repo_id and create_at from the input date"""

    raw_probe = "SELECT repo_id, created_at FROM {0}_dated_repos_comments"
    return raw_probe.format(table_name)


def get_probe_2(table_name='filtered_links_dated'):
    """Create SQL query to select repo_id, linked_repo_id, and created_at from
    table_name."""

    raw_probe = "SELECT repo_id, linked_repo_id, created_at FROM {0}"
    return raw_probe.format(table_name)


def pickled_list(probe_keys):
    """Generate dictionary of project release dates from pickle files

        Files must have the name pattern '<project>_release_dates.p'
        and be pickled dictionarys.
    """

    pickled_release_files = {}
    pickled_release_filebase = "_release_dates.p"
    for key in probe_keys:
        if os.path.isfile(key+pickled_release_filebase):
            pickled_release_files[key] = key + pickled_release_filebase
        else:
            pickled_release_files[key] = None
    return pickled_release_files


def get_probe_results(key_val_pairs, cursor):
    """Fetch query results from MySQL database for each value in key_val_pairs as
    a dictionary"""

    results = {}
    import time
    import mysql.connector
    for key, value in key_val_pairs.items():
        try:
            cursor.execute(value)
        except mysql.connector.Error as err:
            print("\nOops! ", err.msg, "!")
            exit()
        time.sleep(1)
        results[key] = cursor.fetchall()
    return results


def get_raw_releases(key_val_pairs):
    """Fetch unsorted release date information for each value in pairs"""

    results = {}
    for key, value in key_val_pairs:
        if value:
            with open(value, 'rb') as infile:
                results[key] = pickle.load(infile)
        else:
            results[key] = None
    return results


def get_sorted_releases(items):
    """Calculate sorted release, by date of tuple"""

    results = {}
    tmp = get_raw_releases(items)
    for key, value in tmp.items():
        if value:
            results[key] = [tpl[1] for tpl in sorted(value, key=lambda x: x[1])]
        else:
            results[key] = None
    return results


def get_count_issues(results):
    output = {}
    for key in results.keys():
        output[key] = by_week.get_count(results[key])
    return output


def get_count_releases(sorted_dates, results):
    output = {}
    for key, value in sorted_dates.items():
        if value:
            s_date = sorted(results[key], key=lambda x: x[1])[0][1].date()
            e_date = sorted(results[key], key=lambda x: x[1])[-1][1].date()
            output[key] = by_week.get_count_binary(value, s_date, e_date)
        else:
            output[key] = None
    return output


def analyze_probe_1(probe, count, median_mode=True, SD=4):
    output = {}
    for key in probe.keys():
        output[key] = by_week.get_dates(count[key],
                                        probe[key],
                                        median_mode=median_mode,
                                        SD=SD)
    return output


def get_correlation(release_count, issue_count):
    """Get pointbiserial correlation for all repos in the list.

        INPUT:
            --release_count     :       a dict of repo and release dates
            --issue_count       :       a dict of repo and unique-issue counts
    """

    output = {}
    for key, value in release_count.items():
        if value:
            # output[key] = pointbiserialr(value, issue_count[key])
            coefficient, pval = pointbiserialr(value, issue_count[key])
            output[key] = {'coefficient': coefficient, 'pval': pval}
        else:
            output[key] = None
    return output


def use_probe_1(docopt_args, cursor):
    repo_list = docopt_args['<repo>']
    probe_1 = {repo: get_probe_1(repo) for repo in repo_list}
    # probe_2 = get_probe_2()

    if docopt_args['--mean']:
        median_mode = False
    else:
        median_mode = True

    if docopt_args['--p1']:
        # use probe 1
        pickled_release_files = pickled_list(probe_1.keys())
        results_probe_1 = get_probe_results(probe_1, cursor)
        count_issues = get_count_issues(results_probe_1)
        sorted_releases = get_sorted_releases(pickled_release_files.items())
        analyzed_probe_1 = analyze_probe_1(results_probe_1,
                                           count_issues,
                                           median_mode)
        count_releases = get_count_releases(sorted_releases, results_probe_1)
        correlation = get_correlation(count_releases, count_issues)

        return {'PROBE 1': {'The probe': get_probe_1('<repo_name>'),
                            'Analysis': analyzed_probe_1,
                            'Correlations with release dates': correlation}}


def use_probe_2_hindsight(cursor):
        # use probe 2
        '''Get all entries with  (r_id, l_id date) not Null.
        Sort list into sorted sublists of l_ids (each l_id is in a separate
        list in the list).
        For each sub_list of l_ids in the list, if len == 1, delete it,
        otherwise, sort each by date.
        Map all tuples where (date, l.date) in time_frame'''

        # Get all entries that aren't null in r.id, l.id, or date:
        hindsight_probe_2 = {'hindsight probe 2': p2.get_hindsight_probe_2()}
        # Put that all in a dict:
        linked_repos = get_probe_results(hindsight_probe_2, cursor)
        # Pull it out into a list:
        linked_repos = [(a, b, c.date()) for a, b, c
                        in linked_repos['hindsight probe 2'] if c is not None]
        # sort by each linked_repo_id:
        sorted_sub_lrs = p2.ssort(linked_repos, key=lambda x: x[1])
        # eliminate non-repeating l.id sub lists:
        sublist_linked_repos = p2.drop_non_rep(sorted_sub_lrs)
        # get the count of how many repos linked to each ID:
        count_links_to_each_id = [len(e) for e in sublist_linked_repos]
        average_of_link_count = numpy.average(count_links_to_each_id)
        median_of_link_count = numpy.median(count_links_to_each_id)
        SD = numpy.std(count_links_to_each_id)
        return {'list of sublists by linked id': sublist_linked_repos,
                'members info': {
                    'count': count_links_to_each_id,
                    'average count': average_of_link_count,
                    'median count': median_of_link_count,
                    'standard deviation': SD
                    }
                }


def use_probe_2(docopt_args, cursor):
    import datetime
    origin_date = datetime.datetime.now()
    time_count = int(docopt_args['<tc>'])
    time_measure = docopt_args['<tm>']
    submitter = docopt_args['<user>']
    r_ID = int(docopt_args['<rID>'])
    artifact_ID = int(docopt_args['<artID>'])
    the_links = p2.Problem2_5(origin_date, time_count, time_measure, submitter,
                              r_ID, artifact_ID)
    if docopt_args['<rows>']:
        row_count = int(docopt_args['<rows>'])
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

    if docopt_args['--p2'] and docopt_args['--hind']:
        return use_probe_2_hindsight(cursor)
    if docopt_args['--p2'] and not docopt_args['--hind']:
        answer = use_probe_2(docopt_args, cursor)
        print(answer)
        exit()

    if docopt_args['--p1']:
        result = use_probe_1(args, cursor)
        pdict(result)


if __name__ == "__main__":
    from pdict import pdict
    args = docopt(__doc__)
    main(args)
