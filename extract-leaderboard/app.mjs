import mongoose from "mongoose";
import dotenv from "dotenv";
dotenv.config();

// 1,1v1Supremacy
// 2,TeamSupremacy
// 3,Deathmatch
// 4,TeamDeathmatch
const LEADERBOARD_IDS = [1, 2, 3, 4];

const BASE_URL =
  process.env.MYTH_BASE_URL || "https://athens-live-api.worldsedgelink.com/";
const ROUTE = "/community/leaderboard/getLeaderBoard2";
const DEFAULT_PARAMS = {
  platform: "PC_STEAM", // Update as needed
  title: "athens",
  sortBy: "1",
  count: "200",
};

async function fetchLeaderboardData(skip, leaderboardId) {
  const params = {
    ...DEFAULT_PARAMS,
    leaderboard_id: leaderboardId.toString(),
    start: skip.toString(),
  };

  const url = new URL(ROUTE, BASE_URL);
  url.search = new URLSearchParams(params).toString();
  const response = await fetch(url.toString());

  if (!response.ok) {
    throw new Error(`Failed to fetch leaderboard data: ${response.statusText}`);
  }

  return response.json();
}

function mapLeaderboardData(leaderboardStats, statGroups) {
  console.log("Mapping leaderboard data...");
  const leaderboardStatsMap = new Map(
    leaderboardStats.map((stat) => [stat.statgroup_id, stat])
  );

  return statGroups.map((statGroup) => {
    const playerStats = leaderboardStatsMap.get(statGroup.id);
    const totalGames = playerStats.wins + playerStats.losses;
    return {
      ...playerStats,
      id: statGroup.id,
      personal_statgroup_id: statGroup.members[0].personal_statgroup_id,
      profile_id: statGroup.members[0].profile_id,
      level: statGroup.members[0].level,
      name: statGroup.members[0].alias,
      profileUrl: statGroup.members[0].name,
      country: statGroup.members[0].country,
      winPercent: playerStats.wins / totalGames,
      totalGames,
    };
  });
}

const leaderboardPlayerSchema = new mongoose.Schema({
  statgroup_id: { type: Number, index: true },
  leaderboard_id: { type: Number, index: true },
  wins: Number,
  losses: Number,
  streak: Number,
  disputes: Number,
  drops: Number,
  rank: Number,
  ranktotal: Number,
  ranklevel: Number,
  rating: Number,
  regionrank: Number,
  regionranktotal: Number,
  lastmatchdate: Number,
  highestrank: Number,
  highestranklevel: Number,
  highestrating: Number,
  personal_statgroup_id: Number,
  profile_id: Number,
  level: Number,
  name: { type: String, index: true },
  profileUrl: String,
  country: String,
  winPercent: Number,
  totalGames: Number,
});

const LeaderboardPlayers =
  mongoose.models.LeaderboardPlayers ||
  mongoose.model("LeaderboardPlayers", leaderboardPlayerSchema);

async function saveLeaderboardDataToMongo(mappedLeaderboardData) {
  console.log("Saving leaderboard data to MongoDB...");
  await mongoose.connect(process.env.MONGODB_URI, {
    serverSelectionTimeoutMS: 30000, // Increase timeout to 30 seconds
  });

  try {
    const chunkSize = 1000;
    const chunks = [];

    // Split the data into chunks
    for (let i = 0; i < mappedLeaderboardData.length; i += chunkSize) {
      chunks.push(mappedLeaderboardData.slice(i, i + chunkSize));
    }

    // Function to process a chunk
    const processChunk = async (chunk) => {
      const bulkOps = chunk.map((data) => ({
        replaceOne: {
          filter: { statgroup_id: data.statgroup_id, leaderboard_id: data.leaderboard_id},
          replacement: {
            statgroup_id: data.statgroup_id,
            leaderboard_id: data.leaderboard_id,
            wins: data.wins,
            losses: data.losses,
            streak: data.streak,
            disputes: data.disputes,
            drops: data.drops,
            rank: data.rank,
            ranktotal: data.ranktotal,
            ranklevel: data.ranklevel,
            rating: data.rating,
            regionrank: data.regionrank,
            regionranktotal: data.regionranktotal,
            lastmatchdate: data.lastmatchdate,
            highestrank: data.highestrank,
            highestranklevel: data.highestranklevel,
            highestrating: data.highestrating,
            personal_statgroup_id: data.personal_statgroup_id,
            profile_id: data.profile_id,
            level: data.level,
            name: data.name,
            profileUrl: data.profileUrl,
            country: data.country,
            winPercent: data.winPercent,
            totalGames: data.totalGames,
          },
          upsert: true,
        },
      }));

      await LeaderboardPlayers.bulkWrite(bulkOps, { ordered: false });
    };

    // Process all chunks concurrently
    await Promise.all(chunks.map(processChunk));

    console.log("Leaderboard data saved to MongoDB");
  } catch (error) {
    console.error("Error saving to MongoDB", error);
    throw error;
  } finally {
    await mongoose.disconnect();
  }
}

export const lambdaHandler = async (_event, _context) => {
  try {
    for (const leaderboardId of LEADERBOARD_IDS) {
      console.log("Fetching leaderboard data for leaderboard ID:", leaderboardId);
      let skip = 1;
      const leaderboardStats = [];
      const statGroups = [];
      let leaderboardData;

      do {
        leaderboardData = await fetchLeaderboardData(skip, leaderboardId);
        leaderboardStats.push(...leaderboardData.leaderboardStats);
        statGroups.push(...leaderboardData.statGroups);
        skip += 200;
      } while (skip < leaderboardData.rankTotal);

      const mappedLeaderboardData = mapLeaderboardData(
        leaderboardStats,
        statGroups
      );
      await saveLeaderboardDataToMongo(mappedLeaderboardData);
    }

    return {
      statusCode: 200,
      body: JSON.stringify({ message: "Leaderboard extraction successful" }),
    };
  } catch (error) {
    console.error("Leaderboard extraction failed", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ message: "Leaderboard extraction failed" }),
    };
  }
};
