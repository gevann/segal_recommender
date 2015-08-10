from flask import Flask
from flask import render_template
from flask import request
from flask_boostrap import Bootstrap

from invoke import run


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
    result = run('./name_of_script {} {}'.format(request.args.get('arg1', 'arg2')))

    return '{}'.format(result.stdout)

if __name__ == '__main__':
    app.run(debug=True)
