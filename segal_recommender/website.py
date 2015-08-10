from flask import Flask, render_template, request

from invoke import run

app = Flask(__name__)

@app.route('/')
def home():
    return render_template('home.html')

@app.route('/results/', methods=['GET','POST'])
def results():
    # this is where you do stuff
    # saved it changes
    result = run('./name_of_script {} {}'.format(request.args.get('arg1', 'arg2')))

    return '{}'.format(result.stdout)

if __name__ == '__main__':
    app.run(debug=True)
