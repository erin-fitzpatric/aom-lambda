import os
from pymongo import MongoClient
from datetime import datetime, timedelta, timezone, UTC

# auth
username = os.getenv("MONGO_USER")
password = os.getenv("MONGO_PASS")
mongo_url = os.getenv("MONGO_URL")

# mongo params
mongo_uri = f"mongodb+srv://{username}:{password}@{mongo_url}"
client = MongoClient(mongo_uri)
db = client["test"]
collection = db["matches"]

# mongo civs stats pipeline -------------------------------------------
def civ_stats_pipeline(start_date, end_date):
    pipeline =[
    {
        '$match': {
            'matchDate': {
                '$gte': start_date, 
                '$lt': end_date
            }
        }
    }, {
        '$match': {
            'gameMode': '1V1_SUPREMACY'
        }
    }, {
        '$project': {
            'matchHistoryArray': {
                '$objectToArray': '$matchHistoryMap'
            }, 
            'matchDuration': 1, 
            'matchDate': 1, 
            'mapData': 1
        }
    }, {
        '$addFields': {
            'elo_array': '$matchHistoryArray.v.newrating'
        }
    }, {
        '$addFields': {
            'flat_elo_array': {
                '$reduce': {
                    'input': '$elo_array', 
                    'initialValue': [], 
                    'in': {
                        '$concatArrays': [
                            '$$value', '$$this'
                        ]
                    }
                }
            }
        }
    }, {
        '$addFields': {
            'matchHistoryArray.v.avg_elo': {
                '$avg': '$flat_elo_array'
            }
        }
    }, {
        '$addFields': {
            'matchHistoryMap.player0': {
                '$arrayElemAt': [
                    '$matchHistoryArray', 0
                ]
            }, 
            'matchHistoryMap.player1': {
                '$arrayElemAt': [
                    '$matchHistoryArray', 1
                ]
            }
        }
    }, {
        '$addFields': {
            'matchHistoryMap.player0.v.opp_civ_id': '$matchHistoryMap.player1.v.civilization_id', 
            'matchHistoryMap.player1.v.opp_civ_id': '$matchHistoryMap.player0.v.civilization_id'
        }
    }, {
        '$project': {
            'matchHistoryArray': {
                '$objectToArray': '$matchHistoryMap'
            }, 
            'mapData': 1, 
            'matchDate': 1, 
            'matchDuration': 1
        }
    }, {
        '$unwind': {
            'path': '$matchHistoryArray', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$unwind': {
            'path': '$matchHistoryArray.v.v'
        }
    }, {
        '$addFields': {
            'matchHistoryArray.v.v.opp_civ_id': {
                '$arrayElemAt': [
                    '$matchHistoryArray.v.v.opp_civ_id', 0
                ]
            }
        }
    }, {
        '$project': {
            'cleanmatchHistory': '$matchHistoryArray.v.v', 
            'matchDuration': 1, 
            'matchDate': 1, 
            'mapData': 1
        }
    }, {
        '$addFields': {
            'matchDay': {
                '$dateTrunc': {
                    'date': {
                        '$toDate': '$matchDate'
                    }, 
                    'unit': 'day'
                }
            }
        }
    }, {
        '$match': {
            '$expr': {
                '$ne': [
                    '$cleanmatchHistory.civilization_id', '$cleanmatchHistory.opp_civ_id'
                ]
            }
        }
    }, {
        '$set': {
            'cleanmatchHistory.elo_bin': {
                '$switch': {
                    'branches': [
                        {
                            'case': {
                                '$lt': [
                                    '$cleanmatchHistory.avg_elo', 800
                                ]
                            }, 
                            'then': '0-799'
                        }, {
                            'case': {
                                '$lt': [
                                    '$cleanmatchHistory.avg_elo', 1000
                                ]
                            }, 
                            'then': '800-999'
                        }, {
                            'case': {
                                '$lt': [
                                    '$cleanmatchHistory.avg_elo', 1200
                                ]
                            }, 
                            'then': '1000-1199'
                        }, {
                            'case': {
                                '$lt': [
                                    '$cleanmatchHistory.avg_elo', 1400
                                ]
                            }, 
                            'then': '1200-1399'
                        }, {
                            'case': {
                                '$gte': [
                                    '$cleanmatchHistory.avg_elo', 1400
                                ]
                            }, 
                            'then': '1400-2000'
                        }
                    ]
                }
            }
        }
    }, {
        '$facet': {
            'eloBin': [
                {
                    '$group': {
                        '_id': {
                            'civ_id': '$cleanmatchHistory.civilization_id', 
                            'elo_bin': '$cleanmatchHistory.elo_bin', 
                            'matchDay': '$matchDay'
                        }, 
                        'matchDay': {
                            '$first': '$matchDay'
                        }, 
                        'totalResults': {
                            '$sum': 1
                        }, 
                        'avgDuration': {
                            '$avg': '$matchDuration'
                        }, 
                        'totalWins': {
                            '$sum': {
                                '$cond': [
                                    {
                                        '$eq': [
                                            '$cleanmatchHistory.outcome', 1
                                        ]
                                    }, 1, 0
                                ]
                            }
                        }
                    }
                }
            ], 
            'allBin': [
                {
                    '$group': {
                        '_id': {
                            'civ_id': '$cleanmatchHistory.civilization_id', 
                            'elo_bin': 'all', 
                            'matchDay': '$matchDay'
                        }, 
                        'matchDay': {
                            '$first': '$matchDay'
                        }, 
                        'totalResults': {
                            '$sum': 1
                        }, 
                        'avgDuration': {
                            '$avg': '$matchDuration'
                        }, 
                        'totalWins': {
                            '$sum': {
                                '$cond': [
                                    {
                                        '$eq': [
                                            '$cleanmatchHistory.outcome', 1
                                        ]
                                    }, 1, 0
                                ]
                            }
                        }
                    }
                }
            ]
        }
    }, {
        '$project': {
            'combined': {
                '$concatArrays': [
                    '$eloBin', '$allBin'
                ]
            }
        }
    }, {
        '$unwind': {
            'path': '$combined'
        }
    }, {
        '$group': {
            '_id': {
                'elo_bin': '$combined._id.elo_bin', 
                'matchDay': '$combined._id.matchDay'
            }, 
            'totalResultsinEloBracket': {
                '$sum': '$combined.totalResults'
            }, 
            'combinedDocs': {
                '$push': {
                    'totalWins': '$combined.totalWins', 
                    'totalResults': '$combined.totalResults', 
                    'avgDuration': '$combined.avgDuration', 
                    'matchDay': '$combined.matchDay', 
                    '_id': '$combined._id'
                }
            }
        }
    }, {
        '$addFields': {
            'combinedDocs.totalResultsinEloBracket': '$totalResultsinEloBracket'
        }
    }, {
        '$project': {
            'totalResultsinEloBracket': 0, 
            '_id': 0
        }
    }, {
        '$unwind': {
            'path': '$combinedDocs'
        }
    }, {
        '$replaceRoot': {
            'newRoot': '$combinedDocs'
        }
    }, {
        '$addFields': {
            'winRate': {
                '$cond': [
                    {
                        '$eq': [
                            '$totalResults', 0
                        ]
                    }, 0, {
                        '$multiply': [
                            {
                                '$divide': [
                                    '$totalWins', '$totalResults'
                                ]
                            }, 100
                        ]
                    }
                ]
            }
        }
    }, {
        '$addFields': {
            'playRate': {
                '$cond': [
                    {
                        '$eq': [
                            '$totalResultsinEloBracket', 0
                        ]
                    }, 0, {
                        '$multiply': [
                            {
                                '$divide': [
                                    '$totalResults', '$totalResultsinEloBracket'
                                ]
                            }, 100
                        ]
                    }
                ]
            }
        }
    }, {
        '$addFields': {
            'avgDurationMins': {
                '$divide': [
                    '$avgDuration', 60
                ]
            }
        }
    }, {
        '$project': {
            'avgDuration': 0
        }
    }, {
        '$lookup': {
            'from': 'major_gods', 
            'localField': '_id.civ_id', 
            'foreignField': 'id', 
            'as': 'god_info'
        }
    }, {
        '$unwind': '$god_info'
    }, {
        '$addFields': {
            '_id.god_name': '$god_info.name'
        }
    }, {
        '$project': {
            'god_info': 0
        }
    }, {
        '$addFields': {
            '_id.lower_elo': {
                '$switch': {
                    'branches': [
                        {
                            'case': {
                                '$eq': [
                                    '$_id.elo_bin', '0-799'
                                ]
                            }, 
                            'then': 0
                        }, {
                            'case': {
                                '$eq': [
                                    '$_id.elo_bin', '800-999'
                                ]
                            }, 
                            'then': 800
                        }, {
                            'case': {
                                '$eq': [
                                    '$_id.elo_bin', '1000-1199'
                                ]
                            }, 
                            'then': 1000
                        }, {
                            'case': {
                                '$eq': [
                                    '$_id.elo_bin', '1200-1399'
                                ]
                            }, 
                            'then': 1200
                        }, {
                            'case': {
                                '$eq': [
                                    '$_id.elo_bin', '1400-2000'
                                ]
                            }, 
                            'then': 1400
                        }, {
                            'case': {
                                '$eq': [
                                    '$_id.elo_bin', 'all'
                                ]
                            }, 
                            'then': 0
                        }
                    ], 
                    'default': None
                }
            }, 
            '_id.upper_elo': {
                '$switch': {
                    'branches': [
                        {
                            'case': {
                                '$eq': [
                                    '$_id.elo_bin', '0-799'
                                ]
                            }, 
                            'then': 799
                        }, {
                            'case': {
                                '$eq': [
                                    '$_id.elo_bin', '800-999'
                                ]
                            }, 
                            'then': 999
                        }, {
                            'case': {
                                '$eq': [
                                    '$_id.elo_bin', '1000-1199'
                                ]
                            }, 
                            'then': 1199
                        }, {
                            'case': {
                                '$eq': [
                                    '$_id.elo_bin', '1200-1399'
                                ]
                            }, 
                            'then': 1399
                        }, {
                            'case': {
                                '$eq': [
                                    '$_id.elo_bin', '1400-2000'
                                ]
                            }, 
                            'then': 2000
                        }, {
                            'case': {
                                '$eq': [
                                    '$_id.elo_bin', 'all'
                                ]
                            }, 
                            'then': 2000
                        }
                    ], 
                    'default': None
                }
            }
        }
    }, {
        '$addFields': {
            'metaField': '$_id'
        }
    }, {
        '$project': {
            '_id': 0, 
            'metaField.matchDay': 0
        }
    }
    ]

    return pipeline

# --------------------------------------------------


def create_daily_stats(target, ingest_custom_range=False, start_date=None, end_date=None):
    """
    target: (str) name of the collection to insert the documents ie "daily_stats_test"
    ingest_custom_range: (boo) if true user must define start_date and end_date, if false pipeline will 
        only ingest yesterday (in UTC). 
    start_date: (str)  MM/DD/YYYY
    end_date: (str)  MM/DD/YYYY
    """
    
    if ingest_custom_range:
        start_date = start_date.replace(tzinfo=timezone.utc)
        end_date = end_date.replace(tzinfo=timezone.utc)
    else: 
        today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday = today - timedelta(days=1)
        start_date = yesterday
        end_date = today

    print(f"Creating daily stats from {start_date} to {end_date}")
    start_date = start_date.timestamp() * 1000 #milliseconds since epoch
    end_date = end_date.timestamp() * 1000
    
    # define which mongo pipeline we want to run
    pipeline = civ_stats_pipeline(start_date, end_date)
    
    # run the pipeline
    date_range = collection.aggregate(pipeline)
    
    docs = list(date_range)

    # insert docs into target collection
    target.insert_many(docs)

    return docs

# event structure example?
# event = {
#     "target": "stats_pymongo_test",
#     "ingest_custom_range": True,
#     "start_date": '08/26/2024', # one day before aom release = 08/26/2024
#     "end_date": '09/11/2024',
# }


def lambda_handler(event, context):
   
    target = db[event["target"]]
    ingest_custom_range = event["ingest_custom_range"]
    # convert date strings to python datetime objects
    start_date = datetime.strptime(event["start_date"], '%m/%d/%Y')
    end_date = datetime.strptime(event["end_date"], '%m/%d/%Y')

    docs = create_daily_stats(target, ingest_custom_range, start_date, end_date)
    print(f"Created {len(docs)} daily stats documents.")
    return docs

