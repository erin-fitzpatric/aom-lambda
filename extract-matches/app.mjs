import {
  SQSClient,
  ReceiveMessageCommand,
  DeleteMessageCommand,
} from "@aws-sdk/client-sqs";
import mongoose from "mongoose";
import dotenv from "dotenv";
dotenv.config();

const { Schema } = mongoose;

// Initialize SQS client
const sqsClient = new SQSClient({ region: process.env.AWS_REGION });
const QUEUE_URL = process.env.SQS_QUEUE_URL;

export const lambdaHandler = async (_event, _context) => {
  try {
    let messagesExist = true;

    await mongoose.connect(process.env.MONGODB_URI, {
      serverSelectionTimeoutMS: 30000, // Increase timeout to 30 seconds
    });

    while (messagesExist) {
      // Define the parameters for receiving messages
      const receiveParams = {
        QueueUrl: QUEUE_URL,
        MaxNumberOfMessages: 10, // Adjust as needed (1-10)
        WaitTimeSeconds: 20, // Long polling duration
      };

      // Receive messages from SQS queue
      const receiveCommand = new ReceiveMessageCommand(receiveParams);
      const receiveResponse = await sqsClient.send(receiveCommand);

      if (receiveResponse.Messages && receiveResponse.Messages.length > 0) {
        // Process each message
        for (const message of receiveResponse.Messages) {
          console.log("Received message:", message.Body);

          // Process the message
          const matchHistory = await fetchMatchHistory(message.Body);
          const mappedMatchHistory = mapMatchHistoryData(matchHistory);

          // Save matches to MongoDB
          await saveMatchesToMongo(mappedMatchHistory);

          // Delete the message from the queue after processing
          const deleteParams = {
            QueueUrl: QUEUE_URL,
            ReceiptHandle: message.ReceiptHandle,
          };
          const deleteCommand = new DeleteMessageCommand(deleteParams);
          await sqsClient.send(deleteCommand);
        }
      } else {
        console.log("No more messages to process");
        messagesExist = false;
      }
    }

    return {
      statusCode: 200,
      body: JSON.stringify({ message: "All messages processed successfully" }),
    };
  } catch (error) {
    console.error("Match extraction failed", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ message: "Match extraction failed" }),
    };
  }
};

function createRateLimiter(requestsPerSecond) {
  const interval = 1000 / requestsPerSecond;
  let lastExecution = 0;

  return function (fn) {
    return async function (...args) {
      const now = Date.now();
      const timeSinceLastExecution = now - lastExecution;

      if (timeSinceLastExecution < interval) {
        await new Promise((resolve) =>
          setTimeout(resolve, interval - timeSinceLastExecution)
        );
      }

      lastExecution = Date.now();
      return fn(...args);
    };
  };
}

const rateLimiter = createRateLimiter(50);

const fetchMatchHistory = rateLimiter(async function (playerIds) {
  const baseUrl =
    "https://athens-live-api.worldsedgelink.com/community/leaderboard/getRecentMatchHistory";
  const url = `${baseUrl}?title=athens&profile_ids=${encodeURIComponent(
    playerIds
  )}`;

  const response = await fetch(url);
  if (!response.ok) {
    throw new Error("Failed to fetch profile data");
  }

  const data = await response.json();
  return {
    matchHistoryStats: data.matchHistoryStats,
    profiles: data.profiles,
  };
});

function randomMapNameToData(mapName) {
  let data = RandomMapDataMap.get(mapName);
  if (data === undefined) {
    return {
      name: mapName,
      imagePath: "/maps/the_unknown.png",
      isWater: false,
    };
  }
  return data;
}

const RandomMapDataMap = new Map([
  [
    "acropolis",
    {
      name: "Acropolis",
      imagePath: "/maps/acropolis.png",
      isWater: false,
    },
  ],
  [
    "air",
    {
      name: "Aïr",
      imagePath: "/maps/air.png",
      isWater: false,
    },
  ],
  [
    "alfheim",
    {
      name: "Alfheim",
      imagePath: "/maps/alfheim.png",
      isWater: false,
    },
  ],
  [
    "anatolia",
    {
      name: "Anatolia",
      imagePath: "/maps/anatolia.png",
      isWater: true,
    },
  ],
  [
    "archipelago",
    {
      name: "Archipelago",
      imagePath: "/maps/archipelago.png",
      isWater: true,
    },
  ],
  [
    "arena",
    {
      name: "Arena",
      imagePath: "/maps/arena.png",
      isWater: false,
    },
  ],
  [
    "black_sea",
    {
      name: "Black Sea",
      imagePath: "/maps/black_sea.png",
      isWater: true,
    },
  ],
  [
    "blue_lagoon",
    {
      name: "Blue Lagoon",
      imagePath: "/maps/blue_lagoon.png",
      isWater: false,
    },
  ],
  [
    "elysium",
    {
      name: "Elysium",
      imagePath: "/maps/elysium.png",
      isWater: false,
    },
  ],
  [
    "erebus",
    {
      name: "Erebus",
      imagePath: "/maps/erebus.png",
      isWater: false,
    },
  ],
  [
    "ghost_lake",
    {
      name: "Ghost Lake",
      imagePath: "/maps/ghost_lake.png",
      isWater: false,
    },
  ],
  [
    "giza",
    {
      name: "Giza",
      imagePath: "/maps/giza.png",
      isWater: false,
    },
  ],
  [
    "gold_rush",
    {
      name: "Gold Rush",
      imagePath: "/maps/gold_rush.png",
      isWater: false,
    },
  ],
  [
    "highland",
    {
      name: "Highland",
      imagePath: "/maps/highland.png",
      isWater: true,
    },
  ],
  [
    "ironwood",
    {
      name: "Ironwood",
      imagePath: "/maps/ironwood.png",
      isWater: false,
    },
  ],
  [
    "islands",
    {
      name: "Islands",
      imagePath: "/maps/islands.png",
      isWater: true,
    },
  ],
  [
    "jotunheim",
    {
      name: "Jotunheim",
      imagePath: "/maps/jotunheim.png",
      isWater: false,
    },
  ],
  [
    "kerlaugar",
    {
      name: "Kerlaugar",
      imagePath: "/maps/kerlaugar.png",
      isWater: true,
    },
  ],
  [
    "land_unknown",
    {
      name: "Land Unknown",
      imagePath: "/maps/land_unknown.png",
      isWater: false,
    },
  ],
  [
    "marsh",
    {
      name: "Marsh",
      imagePath: "/maps/marsh.png",
      isWater: false,
    },
  ],
  [
    "mediterranean",
    {
      name: "Mediterranean",
      imagePath: "/maps/mediterranean.png",
      isWater: true,
    },
  ],
  [
    "megalopolis",
    {
      name: "Megalopolis",
      imagePath: "/maps/megalopolis.png",
      isWater: false,
    },
  ],
  [
    "midgard",
    {
      name: "Midgard",
      imagePath: "/maps/midgard.png",
      isWater: true,
    },
  ],
  [
    "mirage",
    {
      name: "Mirage",
      imagePath: "/maps/mirage.png",
      isWater: false,
    },
  ],
  [
    "mirkwood",
    {
      name: "Mirkwood",
      imagePath: "/maps/mirkwood.png",
      isWater: false,
    },
  ],
  [
    "mount_olympus",
    {
      name: "Mount Olympus",
      imagePath: "/maps/mount_olympus.png",
      isWater: false,
    },
  ],
  [
    "muspellheim",
    {
      name: "Muspellheim",
      imagePath: "/maps/muspellheim.png",
      isWater: false,
    },
  ],
  [
    "nile_shallows",
    {
      name: "Nile Shallows",
      imagePath: "/maps/nile_shallows.png",
      isWater: false,
    },
  ],
  [
    "nomad",
    {
      name: "Nomad",
      imagePath: "/maps/nomad.png",
      isWater: false,
    },
  ],
  [
    "oasis",
    {
      name: "Oasis",
      imagePath: "/maps/oasis.png",
      isWater: false,
    },
  ],
  [
    "river_nile",
    {
      name: "River Nile",
      imagePath: "/maps/river_nile.png",
      isWater: true,
    },
  ],
  [
    "river_styx",
    {
      name: "River Styx",
      imagePath: "/maps/river_styx.png",
      isWater: true,
    },
  ],
  [
    "savannah",
    {
      name: "Savannah",
      imagePath: "/maps/savannah.png",
      isWater: false,
    },
  ],
  [
    "sea_of_worms",
    {
      name: "Sea of Worms",
      imagePath: "/maps/sea_of_worms.png",
      isWater: true,
    },
  ],
  [
    "team_migration",
    {
      name: "Team Migration",
      imagePath: "/maps/team_migration.png",
      isWater: true,
    },
  ],
  [
    "the_unknown",
    {
      name: "The Unknown",
      imagePath: "/maps/the_unknown.png",
      isWater: true,
    },
  ],
  [
    "tiny",
    {
      name: "Tiny",
      imagePath: "/maps/tiny.png",
      isWater: false,
    },
  ],
  [
    "tundra",
    {
      name: "Tundra",
      imagePath: "/maps/tundra.png",
      isWater: false,
    },
  ],
  [
    "valley_of_kings",
    {
      name: "Valley of Kings",
      imagePath: "/maps/valley_of_kings.png",
      isWater: false,
    },
  ],
  [
    "vinlandsaga",
    {
      name: "Vinlandsaga",
      imagePath: "/maps/vinlandsaga.png",
      isWater: true,
    },
  ],
  [
    "watering_hole",
    {
      name: "Watering Hole",
      imagePath: "/maps/watering_hole.png",
      isWater: false,
    },
  ],
]);

function mapMatchHistoryData({ matchHistoryStats, profiles }) {
  // Sort the match history stats by completion time
  const sortedMatchHistoryStats = matchHistoryStats.sort(
    (a, b) => b.completiontime - a.completiontime
  );

  const profileMap = getProfileMap(profiles);
  const mappedMatchHistoryData = sortedMatchHistoryStats.map((match) => {
    const {
      mapname,
      startgametime,
      completiontime,
      matchhistoryreportresults,
      matchhistorymember,
    } = match;

    // Map the match data
    const parsedMapName = parseMapName(mapname);
    const mapData = randomMapNameToData(parsedMapName);
    const matchDuration = completiontime - startgametime;
    const matchDate = startgametime * 1000;
    const teams = groupAndReorderTeams(matchhistoryreportresults, profileMap);
    const matchHistoryMap = createMatchHistoryMap(matchhistorymember);
    const gameMode =
      getGameModeByMatchTypeId(match.matchtype_id) || "Unknown Game Mode";
    const matchType = match.description;

    return {
      gameMode: gameMode,
      matchType: matchType,
      matchId: match.id,
      mapData,
      matchDate,
      matchDuration,
      teams,
      matchHistoryMap,
    };
  });

  return { mappedMatchHistoryData };
}

const MatchType = {
  CUSTOM: 0,
  ONE_V_ONE_SUPREMACY: 1,
  TWO_V_TWO_SUPREMACY: 2,
  THREE_V_THREE_SUPREMACY: 3,
  FOUR_V_FOUR_SUPREMACY: 4,
  ONE_V_ONE_DEATHMATCH: 5,
  TWO_V_TWO_DEATHMATCH: 6,
  THREE_V_THREE_DEATHMATCH: 7,
  FOUR_V_FOUR_DEATHMATCH: 8,
  ONE_V_ONE_CONQUEST: 9,
  TWO_V_TWO_CONQUEST: 10,
  THREE_V_THREE_CONQUEST: 11,
  FOUR_V_FOUR_CONQUEST: 12,
  ONE_V_ONE_LIGHTNING: 13,
  TWO_V_TWO_LIGHTNING: 14,
  THREE_V_THREE_LIGHTNING: 15,
  FOUR_V_FOUR_LIGHTNING: 16,
  ONE_V_ONE_TREATY_20MIN: 17,
  TWO_V_TWO_TREATY_20MIN: 18,
  THREE_V_THREE_TREATY_20MIN: 19,
  FOUR_V_FOUR_TREATY_20MIN: 20,
  ONE_V_ONE_TREATY_40MIN: 21,
  TWO_V_TWO_TREATY_40MIN: 22,
  THREE_V_THREE_TREATY_40MIN: 23,
  FOUR_V_FOUR_TREATY_40MIN: 24,
  MATCHMAKING: 25,
  MATCHMAKING_QUICKMATCH: 27,
  ONE_V_ONE_SUPREMACY_QUICKMATCH: 28,
  TWO_V_TWO_SUPREMACY_QUICKMATCH: 29,
  THREE_V_THREE_SUPREMACY_QUICKMATCH: 30,
  FOUR_V_FOUR_SUPREMACY_QUICKMATCH: 31,
  ONE_V_ONE_DEATHMATCH_QUICKMATCH: 32,
  TWO_V_TWO_DEATHMATCH_QUICKMATCH: 33,
  THREE_V_THREE_DEATHMATCH_QUICKMATCH: 34,
  FOUR_V_FOUR_DEATHMATCH_QUICKMATCH: 35,
  ONE_V_ONE_LIGHTNING_QUICKMATCH: 36,
  TWO_V_TWO_LIGHTNING_QUICKMATCH: 37,
  THREE_V_THREE_LIGHTNING_QUICKMATCH: 38,
  FOUR_V_FOUR_LIGHTNING_QUICKMATCH: 39,
};

const MatchTypeNames = {
  [MatchType.CUSTOM]: "CUSTOM",
  [MatchType.ONE_V_ONE_SUPREMACY]: "1V1_SUPREMACY",
  [MatchType.TWO_V_TWO_SUPREMACY]: "2V2_SUPREMACY",
  [MatchType.THREE_V_THREE_SUPREMACY]: "3V3_SUPREMACY",
  [MatchType.FOUR_V_FOUR_SUPREMACY]: "4V4_SUPREMACY",
  [MatchType.ONE_V_ONE_DEATHMATCH]: "1V1_DEATHMATCH",
  [MatchType.TWO_V_TWO_DEATHMATCH]: "2V2_DEATHMATCH",
  [MatchType.THREE_V_THREE_DEATHMATCH]: "3V3_DEATHMATCH",
  [MatchType.FOUR_V_FOUR_DEATHMATCH]: "4V4_DEATHMATCH",
  [MatchType.ONE_V_ONE_CONQUEST]: "1V1_CONQUEST",
  [MatchType.TWO_V_TWO_CONQUEST]: "2V2_CONQUEST",
  [MatchType.THREE_V_THREE_CONQUEST]: "3V3_CONQUEST",
  [MatchType.FOUR_V_FOUR_CONQUEST]: "4V4_CONQUEST",
  [MatchType.ONE_V_ONE_LIGHTNING]: "1V1_LIGHTNING",
  [MatchType.TWO_V_TWO_LIGHTNING]: "2V2_LIGHTNING",
  [MatchType.THREE_V_THREE_LIGHTNING]: "3V3_LIGHTNING",
  [MatchType.FOUR_V_FOUR_LIGHTNING]: "4V4_LIGHTNING",
  [MatchType.ONE_V_ONE_TREATY_20MIN]: "1V1_TREATY_20MIN",
  [MatchType.TWO_V_TWO_TREATY_20MIN]: "2V2_TREATY_20MIN",
  [MatchType.THREE_V_THREE_TREATY_20MIN]: "3V3_TREATY_20MIN",
  [MatchType.FOUR_V_FOUR_TREATY_20MIN]: "4V4_TREATY_20MIN",
  [MatchType.ONE_V_ONE_TREATY_40MIN]: "1V1_TREATY_40MIN",
  [MatchType.TWO_V_TWO_TREATY_40MIN]: "2V2_TREATY_40MIN",
  [MatchType.THREE_V_THREE_TREATY_40MIN]: "3V3_TREATY_40MIN",
  [MatchType.FOUR_V_FOUR_TREATY_40MIN]: "4V4_TREATY_40MIN",
  [MatchType.MATCHMAKING]: "MATCHMAKING",
  [MatchType.MATCHMAKING_QUICKMATCH]: "MATCHMAKING_QUICKMATCH",
  [MatchType.ONE_V_ONE_SUPREMACY_QUICKMATCH]: "1V1_SUPREMACY_QUICKMATCH",
  [MatchType.TWO_V_TWO_SUPREMACY_QUICKMATCH]: "2V2_SUPREMACY_QUICKMATCH",
  [MatchType.THREE_V_THREE_SUPREMACY_QUICKMATCH]: "3V3_SUPREMACY_QUICKMATCH",
  [MatchType.FOUR_V_FOUR_SUPREMACY_QUICKMATCH]: "4V4_SUPREMACY_QUICKMATCH",
  [MatchType.ONE_V_ONE_DEATHMATCH_QUICKMATCH]: "1V1_DEATHMATCH_QUICKMATCH",
  [MatchType.TWO_V_TWO_DEATHMATCH_QUICKMATCH]: "2V2_DEATHMATCH_QUICKMATCH",
  [MatchType.THREE_V_THREE_DEATHMATCH_QUICKMATCH]: "3V3_DEATHMATCH_QUICKMATCH",
  [MatchType.FOUR_V_FOUR_DEATHMATCH_QUICKMATCH]: "4V4_DEATHMATCH_QUICKMATCH",
  [MatchType.ONE_V_ONE_LIGHTNING_QUICKMATCH]: "1V1_LIGHTNING_QUICKMATCH",
  [MatchType.TWO_V_TWO_LIGHTNING_QUICKMATCH]: "2V2_LIGHTNING_QUICKMATCH",
  [MatchType.THREE_V_THREE_LIGHTNING_QUICKMATCH]: "3V3_LIGHTNING_QUICKMATCH",
  [MatchType.FOUR_V_FOUR_LIGHTNING_QUICKMATCH]: "4V4_LIGHTNING_QUICKMATCH",
};

// Function to get name by id
function getGameModeByMatchTypeId(id) {
  return MatchTypeNames[id];
}

function createMatchHistoryMap(data) {
  const map = {};

  data.forEach((member) => {
    if (!map[member.profile_id]) {
      map[member.profile_id] = [];
    }
    map[member.profile_id].push(member);
  });
  return map;
}

function getProfileMap(profiles) {
  const profileMap = profiles.reduce((acc, profile) => {
    acc[profile.profile_id] = profile;
    return acc;
  }, {});
  return profileMap;
}

function parseMapName(mapname) {
  const underscoreIndex = mapname.indexOf("_");
  return underscoreIndex !== -1
    ? mapname.substring(underscoreIndex + 1)
    : mapname;
}

function groupAndReorderTeams(results, profiles) {
  // Step 1: Group the results by teamid
  const teamsMap = results.reduce((acc, result) => {
    const { teamid } = result;
    if (!acc[teamid]) {
      acc[teamid] = [];
    }
    acc[teamid].push({
      civilization_id: result.civilization_id,
      matchhistory_id: result.matchhistory_id,
      matchstartdate: result.matchstartdate,
      profile_id: result.profile_id,
      race_id: result.race_id,
      resulttype: result.resulttype,
      teamid: result.teamid,
      xpgained: result.xpgained,
      postgameStats: JSON.parse(result.counters),
      playerName: profiles[result.profile_id].alias,
    });
    return acc;
  }, {});

  // Step 2: Convert the teamsMap object to an array of TeamResult
  const teamsArray = Object.entries(teamsMap).map(([teamid, results]) => ({
    teamid: Number(teamid),
    results,
  }));

  return teamsArray;
}

// Define the schema for the document
const matchSchema = new Schema({
  gameMode: String,
  matchType: String,
  matchId: Number,
  mapData: {
    name: String,
    imagePath: String,
    isWater: Boolean,
  },
  matchDate: Number,
  matchDuration: Number,
  teams: [
    {
      teamid: Number,
      results: [
        {
          civilization_id: Number,
          matchhistory_id: Number,
          matchstartdate: Number,
          profile_id: Number,
          race_id: Number,
          resulttype: Number,
          teamid: Number,
          xpgained: Number,
          postgameStats: {
            mapID: Number,
            postGameAward_HighestScore: Number,
            postGameAward_MostDeaths: Number,
            postGameAward_MostImprovements: Number,
            postGameAward_MostKills: Number,
            postGameAward_MostResources: Number,
            postGameAward_MostTitanKills: Number,
            postGameAward_largestArmy: Number,
            rankedMatch: Number,
            score_Economic: Number,
            score_Military: Number,
            score_Technology: Number,
            score_Total: Number,
            stat_BuildingsRazed: Number,
            stat_UnitsKilled: Number,
            stat_UnitsLost: Number,
          },
          playerName: String,
        },
      ],
    },
  ],
  matchHistoryMap: {
    type: Map,
    of: [
      {
        matchhistory_id: Number,
        profile_id: Number,
        race_id: Number,
        statgroup_id: Number,
        teamid: Number,
        wins: Number,
        losses: Number,
        streak: Number,
        arbitration: Number,
        outcome: Number,
        oldrating: Number,
        newrating: Number,
        reporttype: Number,
        civilization_id: Number,
      },
    ],
  },
});

const Match = mongoose.model("Match", matchSchema);

async function saveMatchesToMongo({ mappedMatchHistoryData }) {
  try {
    const operations = mappedMatchHistoryData.map((match) => ({
      updateOne: {
        filter: { matchId: match.matchId },
        update: { $setOnInsert: match },
        upsert: true,
      },
    }));

    await Match.bulkWrite(operations);
  } catch (error) {
    console.error("Error inserting documents:", error);
    throw new Error("Error inserting documents:", error);
  }
}
