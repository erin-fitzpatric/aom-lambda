import os
from pymongo import MongoClient
from datetime import datetime, timedelta, timezone, UTC

# auth
username = os.getenv("MONGO_USER")
password = os.getenv("MONGO_PASS")
mongo_url = os.getenv("MONGO_URL")
target_str = os.getenv("TARGET_STR")

# mongo params
full_url = f"mongodb+srv://{username}:{password}@{mongo_url}"
client = MongoClient(full_url)
db = client["test"]
collection = db["matches"]

# mongo civs stats pipeline -------------------------------------------
def civ_stats_pipeline(start_date, end_date):
    pipeline = [
    {
        '$match': {
            'gameMode': '1V1_SUPREMACY',
            'matchDate': {
                '$gte': start_date, 
                '$lt': end_date
            }
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
                                    '$cleanmatchHistory.avg_elo', 751
                                ]
                            }, 
                            'then': '0-750'
                        }, {
                            'case': {
                                '$lt': [
                                    '$cleanmatchHistory.avg_elo', 1001
                                ]
                            }, 
                            'then': '751-1000'
                        }, {
                            'case': {
                                '$lt': [
                                    '$cleanmatchHistory.avg_elo', 1251
                                ]
                            }, 
                            'then': '1001-1250'
                        }, {
                            'case': {
                                '$lt': [
                                    '$cleanmatchHistory.avg_elo', 1501
                                ]
                            }, 
                            'then': '1251-1500'
                        }, {
                            'case': {
                                '$lt': [
                                    '$cleanmatchHistory.avg_elo', 1751
                                ]
                            }, 
                            'then': '1501-1750'
                        }, {
                            'case': {
                                '$gte': [
                                    '$cleanmatchHistory.avg_elo', 1751
                                ]
                            }, 
                            'then': '1751-2000'
                        }
                    ]
                }
            }
        }
    }, {
        '$lookup': {
            'from': 'major_gods', 
            'localField': 'cleanmatchHistory.opp_civ_id', 
            'foreignField': 'id', 
            'as': 'opp_god_info'
        }
    }, {
        '$addFields': {
            'cleanmatchHistory.opp_god_name': '$opp_god_info.name'
        }
    }, {
        '$unwind': '$cleanmatchHistory.opp_god_name'
    }, {
        '$project': {
            'opp_god_info': 0, 
            'matchDate': 0
        }
    }, {
        '$facet': {
            'justCivs': [
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
            'oppCivs': [
                {
                    '$group': {
                        '_id': {
                            'civ_id': '$cleanmatchHistory.civilization_id', 
                            'opp_civ_id': '$cleanmatchHistory.opp_god_name', 
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
            'mapData': [
                {
                    '$group': {
                        '_id': {
                            'civ_id': '$cleanmatchHistory.civilization_id', 
                            'elo_bin': '$cleanmatchHistory.elo_bin', 
                            'map_name': '$mapData.name', 
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
        '$unwind': {
            'path': '$justCivs'
        }
    }, {
        '$addFields': {
            'justCivs.mapData': {
                '$filter': {
                    'input': '$mapData', 
                    'as': 'mapDoc', 
                    'cond': {
                        '$and': [
                            {
                                '$eq': [
                                    '$$mapDoc._id.civ_id', '$justCivs._id.civ_id'
                                ]
                            }, {
                                '$eq': [
                                    '$$mapDoc._id.elo_bin', '$justCivs._id.elo_bin'
                                ]
                            }, {
                                '$eq': [
                                    '$$mapDoc._id.matchDay', '$justCivs._id.matchDay'
                                ]
                            }
                        ]
                    }
                }
            }
        }
    }, {
        '$project': {
            'mapData': 0
        }
    }, {
        '$addFields': {
            'justCivs.oppData': {
                '$filter': {
                    'input': '$oppCivs', 
                    'as': 'oppDoc', 
                    'cond': {
                        '$and': [
                            {
                                '$eq': [
                                    '$$oppDoc._id.civ_id', '$justCivs._id.civ_id'
                                ]
                            }, {
                                '$eq': [
                                    '$$oppDoc._id.elo_bin', '$justCivs._id.elo_bin'
                                ]
                            }, {
                                '$eq': [
                                    '$$oppDoc._id.matchDay', '$justCivs._id.matchDay'
                                ]
                            }
                        ]
                    }
                }
            }
        }
    }, {
        '$replaceRoot': {
            'newRoot': '$justCivs'
        }
    }, {
        '$project': {
            '_id': 1, 
            'matchDay': 1, 
            'totalResults': 1, 
            'avgDuration': 1, 
            'totalWins': 1, 
            'mapData._id.map_name': 1, 
            'mapData.totalResults': 1, 
            'mapData.totalWins': 1, 
            'oppData._id.opp_civ_id': 1, 
            'oppData.totalResults': 1, 
            'oppData.totalWins': 1
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
            'matchups': {
                '$arrayToObject': {
                    '$map': {
                        'input': '$oppData', 
                        'as': 'item', 
                        'in': {
                            'k': {
                                '$toString': '$$item._id.opp_civ_id'
                            }, 
                            'v': {
                                'totalResults': '$$item.totalResults', 
                                'totalWins': '$$item.totalWins'
                            }
                        }
                    }
                }
            }
        }
    }, {
        '$addFields': {
            'maps': {
                '$arrayToObject': {
                    '$map': {
                        'input': '$mapData', 
                        'as': 'item', 
                        'in': {
                            'k': {
                                '$toString': '$$item._id.map_name'
                            }, 
                            'v': {
                                'totalResults': '$$item.totalResults', 
                                'totalWins': '$$item.totalWins'
                            }
                        }
                    }
                }
            }
        }
    }, {
        '$project': {
            'mapData': 0, 
            'oppData': 0
        }
    }, {
        '$addFields': {
            '_id.lower_elo': {
                '$switch': {
                    'branches': [
                        {
                            'case': {
                                '$eq': [
                                    '$_id.elo_bin', '0-750'
                                ]
                            }, 
                            'then': 0
                        }, {
                            'case': {
                                '$eq': [
                                    '$_id.elo_bin', '751-1000'
                                ]
                            }, 
                            'then': 751
                        }, {
                            'case': {
                                '$eq': [
                                    '$_id.elo_bin', '1001-1250'
                                ]
                            }, 
                            'then': 1001
                        }, {
                            'case': {
                                '$eq': [
                                    '$_id.elo_bin', '1251-1500'
                                ]
                            }, 
                            'then': 1251
                        }, {
                            'case': {
                                '$eq': [
                                    '$_id.elo_bin', '1501-1750'
                                ]
                            }, 
                            'then': 1501
                        }, {
                            'case': {
                                '$eq': [
                                    '$_id.elo_bin', '1751-2000'
                                ]
                            }, 
                            'then': 1751
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
                                    '$_id.elo_bin', '0-750'
                                ]
                            }, 
                            'then': 750
                        }, {
                            'case': {
                                '$eq': [
                                    '$_id.elo_bin', '751-1000'
                                ]
                            }, 
                            'then': 1000
                        }, {
                            'case': {
                                '$eq': [
                                    '$_id.elo_bin', '1001-1250'
                                ]
                            }, 
                            'then': 1250
                        }, {
                            'case': {
                                '$eq': [
                                    '$_id.elo_bin', '1251-1500'
                                ]
                            }, 
                            'then': 1500
                        }, {
                            'case': {
                                '$eq': [
                                    '$_id.elo_bin', '1501-1750'
                                ]
                            }, 
                            'then': 1750
                        }, {
                            'case': {
                                '$eq': [
                                    '$_id.elo_bin', '1751-2000'
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
            'metaField.matchDay': 0, 
            'avgDuration': 0
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

    Ingesting full time series example: It is Sept 17th, 2024. User wants to ingest all match data 
    since release. Arguments should be as follows:
    ingest_custom_range = True
    start_date = '08/26/2024'
    end_date = '09/17/2024' 
    NOTE: The last full day ingested with these params will be 09/16/2024. Then at 2am UTC on 09/18/2024 
    September 17th will be ingested and the pipeline is off to the races.
    NOTE: Invoke local lambda test with env_vars as: 
    `sam local invoke CivsStatsExtractorFunction --env-vars locals.json`
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

# event structure example
# event = {
#     "ingest_custom_range": True,
#     "start_date": '08/26/2024', # one day before aom release = 08/26/2024
#     "end_date": '09/17/2024',
# }


def lambda_handler(event, context):
    
    print("EVENT DICT-LIKE:")
    print(event)

    target = db[target_str]

    if "ingest_custom_range" in event:
        ingest_custom_range = event["ingest_custom_range"]
    else:
        ingest_custom_range = False

    if "start_date" in event and "end_date" in event:
        # convert date strings to python datetime objects
        start_date = datetime.strptime(event["start_date"], '%m/%d/%Y')
        end_date = datetime.strptime(event["end_date"], '%m/%d/%Y')
    else:
        start_date = None
        end_date = None

    docs = create_daily_stats(target, ingest_custom_range, start_date, end_date)
    print(f"Created {len(docs)} daily stats documents.")

# for running as script in local testing
# if __name__ == "__main__":
#     lambda_handler(event, None)