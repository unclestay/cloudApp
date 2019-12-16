from sshtunnel import SSHTunnelForwarder
from bson import ObjectId, SON
import pymongo
import pprint

MONGO_HOST = "devincimdb2017.westeurope.cloudapp.azure.com"
MONGO_DB = "movieLens"
MONGO_USER = "administrateur"
MONGO_PASS = "V8eOFR%_"

server = SSHTunnelForwarder(
    (MONGO_HOST, 22),
    ssh_username=MONGO_USER,
    ssh_password=MONGO_PASS,
    remote_bind_address=('10.0.0.94', 30000)
)
server.start()

print(server.local_bind_port)  # show assigned local port
# work with `SECRET SERVICE` through `server.local_bind_port`.


client = pymongo.MongoClient('localhost', server.local_bind_port)
db = client.movieLens
collection = db.movie
print("Connected")


# close ssh tunnel


def get_single_movie(movieid):
    data = collection.find_one({"movieid": movieid},
                               {"movieid": 1,
                                "year": 1,
                                "isEnglish": 1,
                                "_id": 0,
                                "country": 1,
                                "runningtime": 1,
                                "director.directorid": 1,
                                "director.avg_revenue": 1,
                                "director.genre": 1})
    return data


def get_single_actor(actorid):
    data = collection.find({"actor.actorid": actorid},
                           {"movieid": 1, "year": 1,
                            "isEnglish": 1, "_id": 0,
                            "director.directorid": 1,
                            "director.avg_revenue": 1,
                            "director.genre": 1})
    movielist = list(data)
    return movielist


def get_english_count(bl):
    data = collection.find({"isEnglish": bl},
                           {"movieid": 1, "year": 1,
                            "isEnglish": 1, "_id": 0,
                            "director.directorid": 1,
                            "director.genre": 1}
                           ).limit(10)
    movielist = list(data)
    return movielist


def get_rating_rank(genre):
    pipeline = [
        {"$match": {"director.genre": genre}},
        {"$unwind": "$user"},
        {"$group": {"_id": "$movieid", "average": {"$avg": "$user.rating"}, "tot": {"$sum": 1}}},
        {"$match": {"tot": {"$gt": 1000}}},
        {"$sort": SON([("average", -1), ("sum", 1)])}]

    rankinglist = list(collection.aggregate(pipeline))
    return rankinglist


def get_user_rank(userid):
    pipeline = [
            {"$match": {"user.userid": userid}},
            {"$unwind": "$user"},
            {"$group": {"_id": "$movieid", "average": {"$avg": "$user.rating"}, "tot": {"$sum": 1}}},
            {"$match": {"tot": {"$gt": 1000}}},
            {"$sort": SON([("average", -1), ("sum", 1)])}]

    rankinglist = list(collection.aggregate(pipeline))
    return rankinglist

