import mongoose from "mongoose";
import dotenv from "dotenv";
dotenv.config();

const BASE_URL =
  process.env.MYTH_BASE_URL || "https://aoe-api.worldsedgelink.com/";
const ROUTE = "/community/leaderboard/getLeaderBoard2";
const DEFAULT_PARAMS = {
  leaderboard_id: "3", // Update as needed
  platform: "PC_STEAM", // Update as needed
  title: "age2", // Update as needed
  sortBy: "1",
  count: "200",
};

async function fetchLeaderboardData(skip) {
  const params = {
    ...DEFAULT_PARAMS,
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
      id: statGroup.id,
      name: statGroup.members[0].alias,
      profileUrl: statGroup.members[0].name,
      country: statGroup.members[0].country,
      rank: playerStats.rank,
      wins: playerStats.wins,
      losses: playerStats.losses,
      winPercent: playerStats.wins / totalGames,
      totalGames,
    };
  });
}

const leaderboardPlayerSchema = new mongoose.Schema({
  id: { type: Number, index: true },
  name: { type: String, index: true },
  profileUrl: String,
  country: String,
  rank: Number,
  wins: Number,
  losses: Number,
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
    const processChunk = async (chunk, index) => {
      const bulkOps = chunk.map((data) => ({
        replaceOne: {
          filter: { id: data.id },
          replacement: {
            id: data.id,
            country: data.country,
            losses: data.losses,
            name: data.name,
            profileUrl: data.profileUrl,
            rank: data.rank,
            totalGames: data.totalGames,
            winPercent: data.winPercent,
          },
          upsert: true,
        },
      }));

      await LeaderboardPlayers.bulkWrite(bulkOps, { ordered: false });
    };

    // Process all chunks concurrently
    await Promise.all(chunks.map((chunk, index) => processChunk(chunk, index)));

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
    console.log("Fetching leaderboard data...");
    let skip = 1;
    const leaderboardStats = [];
    const statGroups = [];
    let leaderboardData;

    do {
      leaderboardData = await fetchLeaderboardData(skip);
      leaderboardStats.push(...leaderboardData.leaderboardStats);
      statGroups.push(...leaderboardData.statGroups);
      skip += 200;
    } while (skip < leaderboardData.rankTotal);

    const mappedLeaderboardData = mapLeaderboardData(
      leaderboardStats,
      statGroups
    );
    await saveLeaderboardDataToMongo(mappedLeaderboardData);

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