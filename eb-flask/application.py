try:
    from flask import Flask, render_template, abort, request, redirect, jsonify, url_for
except Exception as e:
    print e

application = Flask(__name__)

@application.route('/')
def index(): return render_template('index.html')

if __name__ == '__main__':
    application.run()
