import os
from pymongo import MongoClient, ASCENDING
from datetime import datetime, timedelta, UTC
import itertools

# params
ELO_CUTOFFS = [
    [0, 750],
    [751, 1000],
    [1001, 1250],
    [1251, 1500],
    [1501, 1750],
    [1751, 2000],
]

# auth
username = os.getenv("MONGO_USER")
password = os.getenv("MONGO_PASS")
mongo_url = os.getenv("MONGO_URL")
target_str = os.getenv("TARGET_STR")

full_url = f"mongodb+srv://{username}:{password}@{mongo_url}"

client = MongoClient(full_url)
db = client["test"]
matches = db["matches"]
builds = db["builds"]


# programmaticaly generate elo bins within the query: 
def generate_elo_bins(elo_cutoffs):
    branch_list = []
    for i in elo_cutoffs[:-1]:
        elo_string = f"{i[0]}-{i[1]}"
        branch_dict = {'case': {'$lt': ['$cleanmatchHistory.avg_elo', i[1]]}, 'then': elo_string}
        branch_list.append(branch_dict)
    last_string = f"{elo_cutoffs[-1][0]}-{elo_cutoffs[-1][1]}"
    branch_list.append({'case': {'$gte': ['$cleanmatchHistory.avg_elo', elo_cutoffs[-1][0]]}, 'then': last_string})

    return branch_list


def generate_int_elo_bounds(elo_cutoffs, bound):
    # bound = 0 for lower 
    # bound = 1 for upper
    branch_list = []
    for i in elo_cutoffs:
        elo_string = f"{i[0]}-{i[1]}"
        branch_dict = {'case': {'$eq': ['$_id.elo_bin', elo_string]}, 'then': i[bound]}
        branch_list.append(branch_dict)

    return branch_list


# mongo pipeline -------------------------------------------
def civs_stats_pipeline(patch_start, patch_end, build_number):
    pipeline = [
    {
        '$match': {
            'gameMode': '1V1_SUPREMACY', 
            'matchDate': {
                '$gte': patch_start, 
                '$lt': patch_end
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
                    'branches': generate_elo_bins(ELO_CUTOFFS)
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
                            'elo_bin': '$cleanmatchHistory.elo_bin'
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
                            'elo_bin': '$cleanmatchHistory.elo_bin'
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
                            'map_name': '$mapData.name'
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
                    'branches': generate_int_elo_bounds(ELO_CUTOFFS, 0), 
                    'default': None
                }
            }, 
            '_id.upper_elo': {
                '$switch': {
                    'branches': generate_int_elo_bounds(ELO_CUTOFFS, 1), 
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
    }, {
        '$addFields': {
            'metaField.buildNumber': build_number
        }
    }
    ]

    return pipeline
# --------------------------------------------------------


def upsert_to_mongo(target, docs): 
    for doc in docs:
        query = {"metaField.civ_id": doc["metaField"]["civ_id"],
                 "metaField.elo_bin": doc["metaField"]["elo_bin"],
                 "metaField.buildNumber": doc["metaField"]["buildNumber"]}
        
        target.replace_one(query, doc, upsert=True)


def create_stats_by_patch(target, ingest_all=False):
    """
    Target (str): Name of the collection to upsert to
    ingest_all (bool): True to ingest all builds, False (default) to only ingest latest build
    """

    build_cursor = builds.find().sort('releaseDate', ASCENDING) 
    build_docs = list(build_cursor)

    build_dict = build_docs.copy()
    for i, build in enumerate(build_docs[:-1]): # iterate through builds, set end to 1 second before next build
        build_dict[i]['endDate'] = build_docs[i + 1]["releaseDate"] - timedelta(seconds=1) # calc end date for build, should probably be in db

        # latest build endtime is now
        build_dict[-1]['endDate'] = datetime.now(UTC)


    if ingest_all:
        print("Ingesting all builds...")
        docs_list = []
        for build in build_dict: # get all builds
            
            build_number = build["buildNumber"]
            build_name = build["description"]
            build_release = build["releaseDate"].timestamp() * 1000
            build_end = build["endDate"].timestamp() * 1000
            print(f"Rolling up stats for build {build_number} -- {build_name}")

            pipeline = civs_stats_pipeline(
                build_release, 
                build_end, 
                build_number,
            )
            cursor = matches.aggregate(pipeline)
            docs = list(cursor)
            
            upsert_to_mongo(target, docs)
            print(f"Created {len(docs)} from {build_name}")
            docs_list.append(docs)
        
        return list(itertools.chain(*docs_list)) # just to get total len of docs
        
    else: # only get latest [-1] build
        print("Only ingesting latest build...")
        build_number = build_dict[-1]["buildNumber"]
        build_name = build_dict[-1]["description"]
        build_release = build_dict[-1]["releaseDate"].timestamp() * 1000
        build_end = build_dict[-1]["endDate"].timestamp() * 1000
        print(f"Rolling up stats for build {build_number} -- {build_name}")

        pipeline = civs_stats_pipeline(
            build_release, 
            build_end, 
            build_number,
            ) 
        cursor = matches.aggregate(pipeline)
        docs = list(cursor)

        upsert_to_mongo(target, docs)
        return docs

# example event   
# event = {
#     "ingest_all": False,
# }

def lambda_handler(event, context):
    print("EVENT DICT-LIKE:")
    print(event)

    target = db[target_str]

    if "ingest_all" in event:
        ingest_all = event["ingest_all"]
    else:
        ingest_all = False
    
    docs = create_stats_by_patch(target, ingest_all)
    print(f"Created {len(docs)} stats documents grouped by patch")

# for running as script in local testing
# if __name__ == "__main__":
#     lambda_handler(event, None)