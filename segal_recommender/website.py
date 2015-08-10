from flask import Flask
from flask import render_template
from flask import request
from flask_bootstrap import Bootstrap

# from invoke import run


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
    # this is where you do stuff
    # saved it changes
    # result = run('./name_of_script {tc} {tm} {user} {rID} {artID}{rows}'.format(request.args.get(**data)))
    user = get_arg('user')
    rID = get_arg('rID')
    artID = get_arg('artID')
    time_count = get_arg('tc')
    time_measure = get_arg('tm')
    rows = get_arg('rows')
    data = [user, rID, artID, time_count, time_measure, rows]
    return '{}'.format(", ".join(data))
    # return '{}'.format(result.stdout)


def get_arg(arg_name):
    return request.args.get(arg_name)

if __name__ == '__main__':
    app.run(debug=True)
