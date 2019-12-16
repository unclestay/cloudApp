from flask import Flask, render_template, request, redirect, url_for, Blueprint, flash
from ssh import get_single_movie, get_single_actor, get_english_count, get_rating_rank, get_user_rank, remove_data, \
    updata_existing, insert_data, get_shard_status, get_chunk_status
from flask_wtf import Form
from flask_login import LoginManager, login_user, logout_user, current_user, login_required
from wtforms import StringField, SubmitField
from wtforms.validators import DataRequired
from models import User, query_user
import os

app = Flask(__name__)

SECRET_KEY = os.urandom(32)
app.config['SECRET_KEY'] = SECRET_KEY
login_manager = LoginManager()
login_manager.login_view = 'login'
login_manager.login_message_category = 'info'
login_manager.init_app(app)


@login_manager.user_loader
def load_user(user_id):
    if query_user(user_id) is not None:
        curr_user = User()
        curr_user.id = user_id

        return curr_user


@app.route('/')
@login_required
def index():
    return 'Logged in as: %s' % current_user.get_id()


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        user_id = request.form.get('userid')
        user = query_user(user_id)
        if user is not None and request.form['password'] == user['password']:
            curr_user = User()
            curr_user.id = user_id

            login_user(curr_user)

            return redirect(url_for('inputMovieid'))

        flash('Wrong username or password!')

    return render_template('login.html')


@app.route('/logout')
@login_required
def logout():
    logout_user()
    return 'Logged out successfully!'


class MyForm(Form):
    name = StringField(label='Name')
    movie = SubmitField(label='movie')
    actor = SubmitField(label='actor')
    english = SubmitField(label='english')
    rating = SubmitField(label='rating')
    user = SubmitField(label='user')
    removetext = StringField(label='removetext')
    remove = SubmitField(label='remove')
    updateid = StringField(label='updataid')
    updatedata = StringField(label='updatadata')
    update = SubmitField(label='update')
    inserttext = StringField(label='inserttext')
    insert = SubmitField(label='insert')
    logout = SubmitField(label='logout')
    analyst = SubmitField(label='analyst')


@app.route('/index', methods=['POST', 'GET'])
def inputMovieid():
    form = MyForm()
    if request.method == "POST":
        if form.validate_on_submit():
            if form.movie.data:
                movieid = request.form['name']
                return redirect(url_for('getSingleMovie', movieid=movieid))
            elif form.actor.data:
                actorid = request.form['name']
                return redirect(url_for('getSingleActor', actorid=actorid))
            elif form.english.data:
                bl = request.form['name']
                return redirect(url_for('getEnglishCount', bl=bl))
            elif form.rating.data:
                genre = request.form['name']
                return redirect(url_for('getRatingRank', genre=genre))
            elif form.user.data:
                userid = request.form['name']
                return redirect(url_for('getUserRank', userid=userid))
            elif form.remove.data:
                removeid = request.form['removetext']
                return redirect(url_for('remove', removeid=removeid))
            elif form.update.data:
                updateid = request.form['updateid']
                updatedata = request.form['updatedata']
                return redirect(url_for('updateExisting', updateid=updateid, updatedata=updatedata))
            elif form.insert.data:
                insertdata = request.form['inserttext']
                return redirect(url_for('insert', insertdata=insertdata))
            elif form.logout.data:
                return redirect(url_for('logout'))
            elif form.analyst.data:
                return redirect(url_for('getShardingStatus'))
            else:
                print("failed")
    return render_template("index.html", form=form)


@app.route('/singleMovie/<movieid>', methods=["POST", "GET"])
def getSingleMovie(movieid):
    singleMovie = get_single_movie(int(movieid))
    return render_template('singleMovie.html', singleMovie=singleMovie)


@app.route('/singleActor/<actorid>', methods=["POST", "GET"])
def getSingleActor(actorid):
    singleActor = get_single_actor(int(actorid))
    return render_template('singleActor.html', singleActor=singleActor)


@app.route('/englishCount/<bl>', methods=["POST", "GET"])
def getEnglishCount(bl):
    englishCount = get_english_count(str(bl))
    return render_template('englishCount.html', englishCount=englishCount)


@app.route('/ratingRank/<genre>', methods=["POST", "GET"])
def getRatingRank(genre):
    ratingRank = get_rating_rank(str(genre))
    return render_template('ratingRank.html', ratingRank=ratingRank)


@app.route('/userRank/<userid>', methods=["POST", "GET"])
def getUserRank(userid):
    userRank = get_user_rank(int(userid))
    return render_template('userRank.html', userRank=userRank)


@app.route('/remove/<removeid>', methods=["POST", "GET"])
@login_required
def remove(removeid):
    removedData = remove_data(int(removeid))
    return render_template('remove.html', removedData=removedData)


@app.route('/update/<updateid>/<updatedata>', methods=["POST", "GET"])
@login_required
def updateExisting(updateid, updatedata):
    updateExist = updata_existing(str(updateid), str(updatedata))
    return render_template('update.html', updateExist=updateExist)


@app.route('/insert/<insertdata>', methods=["POST", "GET"])
@login_required
def insert(insertdata):
    insertedData = insert_data(dict(insertdata))
    return render_template('insert.html', insertedData=insertedData)


@app.route('/analyst', methods = ["POST", "GET"])
@login_required
def getShardingStatus():
    shardingStatus = get_shard_status()
    chunkStatus = get_chunk_status()
    return render_template('analyst.html', shardingStatus=shardingStatus, chunkStatus=chunkStatus)


if __name__ == '__main__':
    app.run(debug=True, port=5000)
