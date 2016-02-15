from datetime import timedelta
import datetime


def ssort(ls, key=None):
    '''Sort ls into sublists of matching elements

    INPUT
        ls  :   a list to sort
        key :   a function to access elements of the list, fault = None
    '''

    out = []
    prev = None
    if not key:
        ls.sort()
        for element in ls:
            if element == prev:
                out[-1].append(element)
            else:
                out.append([element])
                prev = element
    else:
        ls.sort(key=key)
        for element in ls:
            if key(element) == prev:
                out[-1].append(element)
            else:
                out.append([element])
                prev = key(element)
    return out


def drop_non_rep(ls):
    '''Remove sub_list with only 1 element'''

    assert ls != [], 'list must be none empty'
    out = []
    for sub_ls in ls:
        if len(sub_ls) > 1:
            out.append(sub_ls)
    assert out != [], 'list must contain at least 1 non-empty sub-list'
    return out


def get_probe_2(source, target, the_date, table='filtered_links_dated',
                time_frame={'window': 1, 'window_type': 'week'}):
    '''Get SQL query for all tuples which also linked to "to" repo within a
    given time frame.

    INPUT
        source      :   an integer repo id
        target      :   an integer repo id
        the_date    :   a date object, from-repos creation date
        table       :   string name of table to query,
                        default = 'filtered_links_dated'
        time_frame  :   a dict which defines the time frame,
                        default = {'window': 1, 'window_type': 'week'}

    OUTPUT
        a list of (owner_id, linked_owner_id, repo_id, linked_repo_id, date)
        tuples.
    '''

    query = "SELECT owner_id, linked_owner_id, repo_id, linked_repo_id, "\
        "date FROM %s WHERE (repo_id != %s) "\
        "AND (linked_repo_id = %s)"
    inputs = (table, source, target)

    return query, inputs


def get_hindsight_probe_2():
    '''Get sql to fetch all entries from filtered_links_dated table wher
    repo_id, linked_repo_id, and date are all non-null.

    This probe is used in hindsight to search for examples of when two repos
    had the same problem.'''

    qry = "SELECT repo_id, linked_repo_id, date "\
        "FROM filtered_links_dated "\
        "WHERE (repo_id IS NOT NULL) "\
        "AND (linked_repo_id IS NOT NULL) "\
        "AND (date IS NOT NULL)"
    return qry


class Problem1:
    '''Answer problem 1:

        "Given any newly-created link to a repo B,
         locate and display all repos Xi
         where each Xi already has a link to B."'''

    def __init__(self):
        '''Construct a Problem1 object'''
        pass

    def find_link_qry(self, ID, submitter, date_mode=False):
        '''Given any repo ID, find and return a list of all owner/repo combos
        which have already linked to the same ID.

        INPUT:
            ID  :   An integer repository ID that was linked to.
        OUTPUT
            A list of (owner, repo, issue/commit) tuples'''

        qry = "SELECT DISTINCT owner, repo, number, count_ii, count_ic, "\
            "count_ci, count_cc%s FROM filtered_links_dated "\
            "WHERE linked_repo_id = %s "\
            "AND owner != %s"\
            "%s"
        if date_mode:
            use_date = ', date'
            and_date = " AND date IS NOT NULL AND date > 0 "\
                "GROUP BY (owner)"
        else:
            use_date = ''
            and_date = ''

        inputs = (submitter, use_date, ID, and_date)
        return qry, inputs

    def find_links(self, ID, submitter, cursor, row_count=3, date_mode=False):
        '''Get 3 owner, repo, number, link-type tuples linking to the same ID

        INPUT
                ID          :       int; repo_id being linked to.
                submitter   :       string; name of linker.
                cursor      :       MySQLCursor object; linked to database
                                    with filtered_links_dated table
                row_count   :       int; number of results desired.
                                    Default = 3
        '''
        if date_mode:
            qry, inputs = self.find_link_qry(ID, submitter, date_mode=date_mode)
        else:
            qry, inputs = self.find_link_qry(ID, submitter)
        cursor.execute(qry, inputs)
        return cursor.fetchmany(row_count)

    def get_urls(self, ID, submitter, cursor, row_count=3):
        '''Get urls for each other owner/repo who linked to the same ID'''

        base_url = 'http://www.github.com/{owner}/{repo}/{mode}/{number}'
        link_data = self.find_links(ID, submitter, cursor, row_count)
        assert link_data != [], 'No tuples match your query!'
        output = []
        for tpl in link_data:
            info = {}
            if tpl[5] or tpl[6]:
                info['mode'] = 'commits'
            else:
                info['mode'] = 'issues'
            info['number'] = tpl[2]
            info['owner'] = tpl[0]
            info['repo'] = tpl[1]
            url = base_url.format(**info)
            output.append(url)
        return output


class Problem2(Problem1):
    '''Answer problem 2:

        "Given any newly-created link to a repo B,
         and a time window Y, locate and display
         all repos Xi where each Xi already has
         a link to B."'''

    TIME_WINDOW_TYPE_MAP = {
        'day': 1, 'week': 7, 'month': 30, 'year': 365,
        'days': 1, 'weeks': 7, 'months': 30, 'years': 365
    }

    def __init__(self, origin_date, time_count, time_measure, submitter, ID):
        assert type(time_count) == int, 'amount of time must be int'
        assert type(time_measure) == str, 'unit of time must be a string'
        assert isinstance(origin_date, datetime.date), 'origin must be a '\
            'date object'
        assert time_measure in self.TIME_WINDOW_TYPE_MAP.keys(), 'unit of time'\
            ' must be "day(s)", "week(s)", "month(s)", or "year(s)".'
        assert type(submitter) == str, 'submitter name must be a string'
        assert type(ID) == int, 'ID must be an int'
        self.origin_date = origin_date
        self.nothing_found = False
        self.submitter = submitter
        self.time_count = time_count
        self.time_measure = time_measure.strip('s')
        self.time_window = self.get_time_window(origin_date,
                                                time_count, time_measure)
        self.ID = ID
        self.qry, self.inputs = self.find_link_qry()
        self.row_count = 3

    def set_row_count(self, new):
        '''Override default 3 rows returned for links'''

        assert type(new) == int, 'Can only override with integer values'
        assert new > 0, 'Value must be between 1 and 20'
        assert new < 21, 'Value must be between 1 and 20'
        self.row_count = new

    def find_links(self, cursor):
        '''Get 3 owner, repo, number, link-type tuples lining to the same ID
        within the time frame'''

        cursor.execute(self.qry, self.inputs)
        self.link_data = cursor.fetchmany(self.row_count)
        return self.link_data

    def find_link_qry(self):
        '''Make MySQL query to repos who also linked to ID within a timeframe'''

        qry = "SELECT DISTINCT owner, repo, number, count_ii, count_ic, "\
            "count_ci, count_cc, date "\
            "FROM filtered_links_dated "\
            "WHERE linked_repo_id = %s "\
            "AND owner != linked_owner "\
            "AND (date BETWEEN %s AND %s) "\
            "GROUP BY (owner)"
        inputs = (self.ID, self.time_window, self.origin_date)
        return qry, inputs

    def get_time_window(self, origin_date, amount, measure):
        '''Create date object amount measures before origin_date'''

        assert measure in self.TIME_WINDOW_TYPE_MAP.keys(), 'measure not valid'
        num_days = amount * self.TIME_WINDOW_TYPE_MAP[measure]
        return origin_date - timedelta(days=num_days)

    def in_time_window(self, d1, d2, window=1, window_type='week'):
        '''Calculate if d1 and d2 are within "window" "window_types"s of each
        other'''

        delta = abs((d1 - d2).days)
        timeframe = window * self.TIME_WINDOW_TYPE_MAP[window_type]
        return delta <= timeframe

    def get_all_in_time_window(self, lst, window=1, window_type='week'):
        '''Get tuples that are within <window> <window_types> of each other'''

        out = []
        for i, tup in enumerate(lst):
            d1 = tup[2]
            for other_tup in lst[(i+1):]:
                d2 = other_tup[2]
                if self.in_time_window(d1, d2, window, window_type):
                    if tup not in out:
                        out = out + [tup, other_tup]
                    else:
                        out.append(other_tup)
        return out

    @staticmethod
    def mode(ID):
        '''Get type of artifact being linked to'''

        try:
            int(ID)
        except ValueError:
            return 'commits'
        return 'issues'

    def set_urls(self, cursor):
        '''Set urls for each other owner/repo who linked to the same ID'''

        base_url = 'http://www.github.com/{owner}/{repo}/{mode}/{number}'
        try:
            self.find_links_strict(cursor)
        except AttributeError:
            self.find_links(cursor)
        if self.link_data == []:
            self.nothing_found = True
        output = []
        for tpl in self.link_data:
            info = {}
            info['mode'] = Problem2.mode(tpl[2])
            info['number'] = tpl[2]
            info['owner'] = tpl[0]
            info['repo'] = tpl[1]
            url = base_url.format(**info)
            output.append(url)
        self.urls = output
        return self.urls


class Problem2_5(Problem2):
    '''Given any newly-created link to an issue/commit A in repo B  and a time
    window Y, locate and display all repos Xi where each Xi already has a link
    to B, with a time window of Y backwards.

    USAGE:
        my_obj = probe2.Problem2_5(origin_date, time_count, time_measure,
                                   submitter, r_ID, artifact_ID)
        my_obj.set_urls()'''

    def __init__(self, origin_date, time_count, time_measure, submitter, r_ID,
                 artifact_ID):
        '''Create a Problem2_5 object.

        INPUT:
            origin_date     :       a datetime object
            time_count      :       an integer amount of time
            time_measure    :       a string unit of time. Default = 'week'
                                    Valid for: day, week , month, year
            submitter       :       a string of the submitters GitHub name
            r_ID            :       an integer of the repo being linked to.
            artifact_ID     :       an integer or string of the id of the
                                    commit or issue being linked to.'''

        assert type(time_count) == int, 'amount of time must be int'
        assert type(time_measure) == str, 'unit of time must be a string'
        time_measure = time_measure.strip()
        assert isinstance(origin_date, datetime.date), 'origin must be a '\
            'date object'
        assert time_measure in self.TIME_WINDOW_TYPE_MAP.keys(), 'unit of time'\
            ' must be "day(s)", "week(s)", "month(s)", or "year(s)". Got {}'.format(time_measure)
        assert type(submitter) == str, 'submitter name must be a string'
        assert type(r_ID) == int, 'ID must be an int'

        if type(artifact_ID) == int:
            self.artifact_ID = str(artifact_ID)
        elif type(artifact_ID) == str:
            self.artifact_ID = artifact_ID
        else:
            raise AssertionError('artifact ID must be an int or string')

        self.origin_date = origin_date
        self.used_non_strict = False
        self.submitter = submitter
        self.time_count = time_count
        self.time_measure = time_measure.strip('s')
        self.time_window = self.get_time_window(origin_date,
                                                time_count, time_measure)
        self.ID = r_ID
        self.strict_qry, self.strict_inputs = self.find_link_qry_strict()
        self.qry, self.inputs = self.find_link_qry()
        self.row_count = 3
        self.nothing_found = False

    def find_link_qry_strict(self):
        '''Make MySQL query to repos who also linked to ID within a timeframe'''

        qry = "SELECT DISTINCT owner, repo, number, linked_number, "\
            "count_ii, count_ic, "\
            "count_ci, count_cc, date "\
            "FROM filtered_links_dated "\
            "WHERE linked_repo_id = {0} "\
            "AND linked_number = '{1}' "\
            "AND owner != linked_owner "\
            "AND (date BETWEEN '{2}' AND '{3}') "\
            "GROUP BY (owner)"

        inputs = (self.ID, self.artifact_ID, self.time_window, self.origin_date)
        return qry, inputs

    def find_links_strict(self, cursor):
        '''Get 3 owner, repo, number, link-type tuples lining to the same ID
        within the time frame IFF they also linked to the same artifact ID'''

        cursor.execute(self.strict_qry, self.strict_inputs)
        self.link_data = cursor.fetchmany(self.row_count)
        if len(self.link_data) == 0:
            self.used_non_strict = True
            cursor.execute(self.qry, self.inputs)
            self.link_data = cursor.fetchmany(self.row_count)
        return self.link_data

    def __str__(self):
        nonstrict = "\nSorry, we couldn't find a match to that exact item. "\
            "Below are the links just to repository {0}.\n".format(self.ID)
        output = "{0}\nUrls You Might Want To Check Out:\n\n"
        if self.used_non_strict:
            output = output.format(nonstrict)
        else:
            output = output.format("")
        for e in self.urls:
            output += ('\t' + e + '\n')
        return output
