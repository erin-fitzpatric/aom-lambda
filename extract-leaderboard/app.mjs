import mongoose from "mongoose";
import dotenv from "dotenv";
dotenv.config();

const BASE_URL = process.env.MYTH_BASE_URL || "https://aoe-api.worldsedgelink.com/";
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
  const leaderboardStatsMap = new Map(
    leaderboardStats.map((stat) => [stat.statgroup_id, stat])
  );

  return statGroups.map((statGroup) => {
    const playerStats = leaderboardStatsMap.get(statGroup.id);
    const totalGames = playerStats.wins + playerStats.losses;
    return {
      id: playerStats.id,
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

async function saveLeaderboardDataToMongo(mappedLeaderboardData) {
  try {
    await mongoose.connect(process.env.MONGODB_URI);

    // drop collection if it exists
    await mongoose.connection.db.dropCollection("leaderboardplayers").catch(() => {});

    const leaderboardPlayerSchema = new mongoose.Schema({
      id: Number,
      name: { type: String, index: true },
      profileUrl: String,
      country: String,
      rank: Number,
      wins: Number,
      losses: Number,
      winPercent: Number,
      totalGames: Number,
    });

    const LeaderboardPlayers = mongoose.model("LeaderboardPlayers", leaderboardPlayerSchema);
    await LeaderboardPlayers.insertMany(mappedLeaderboardData);
    console.log("Saved to MongoDB successfully");
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

    const mappedLeaderboardData = mapLeaderboardData(leaderboardStats, statGroups);
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
