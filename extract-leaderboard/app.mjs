import mongoose from "mongoose";
import dotenv from "dotenv";
import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";

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

// Initialize the SQS client
const sqs = new SQSClient({ region: process.env.AWS_REGION });

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

    // Track profile_ids with increased totalGames
    const profileIdsWithIncreasedTotalGames = new Set();

    // Function to process a chunk
    const processChunk = async (chunk) => {
      // Fetch existing documents
      const existingDocuments = await LeaderboardPlayers.find({
        statgroup_id: { $in: chunk.map((data) => data.statgroup_id) },
        leaderboard_id: { $in: chunk.map((data) => data.leaderboard_id) },
      }).lean();

      const existingMap = new Map(
        existingDocuments.map((doc) => [
          `${doc.statgroup_id}_${doc.leaderboard_id}`,
          doc,
        ])
      );

      const bulkOps = chunk.map((data) => {
        const key = `${data.statgroup_id}_${data.leaderboard_id}`;
        const existingDoc = existingMap.get(key);

        // Check if totalGames increased
        if (existingDoc && existingDoc.totalGames < data.totalGames) {
          profileIdsWithIncreasedTotalGames.add(data.profile_id);
        }

        return {
          replaceOne: {
            filter: {
              statgroup_id: data.statgroup_id,
              leaderboard_id: data.leaderboard_id,
            },
            replacement: data,
            upsert: true,
          },
        };
      });

      await LeaderboardPlayers.bulkWrite(bulkOps, { ordered: false });
    };

    // Process all chunks concurrently
    await Promise.all(chunks.map(processChunk));
    console.log("Leaderboard data saved to MongoDB");

    // Log profile_ids with increased totalGames
    console.log(
      "Profile IDs with increased totalGames:",
      Array.from(profileIdsWithIncreasedTotalGames)
    );
    return Array.from(profileIdsWithIncreasedTotalGames);
  } catch (error) {
    console.error("Error saving to MongoDB", error);
    throw error;
  } finally {
    await mongoose.disconnect();
  }
}

async function sendMessagesToSQS(profileIds) {
  console.log("Sending message to SQS...");

  // Convert the profile IDs array to a JSON string
  const messageBody = JSON.stringify(profileIds);

  const params = {
    QueueUrl: process.env.SQS_QUEUE_URL,
    MessageBody: messageBody,
  };

  try {
    const command = new SendMessageCommand(params);
    await sqs.send(command);
    console.log(`Pushed ${profileIds.length} messages to SQS`);
  } catch (error) {
    console.error("Error sending message to SQS", error);
  }
}

function chunkArray(array, size) {
  const result = [];
  for (let i = 0; i < array.length; i += size) {
    result.push(array.slice(i, i + size));
  }
  return result;
}

export const lambdaHandler = async (_event, _context) => {
  try {
    for (const leaderboardId of LEADERBOARD_IDS) {
      console.log(
        "Fetching leaderboard data for leaderboard ID:",
        leaderboardId
      );
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
      const updatedProfileIds = await saveLeaderboardDataToMongo(
        mappedLeaderboardData
      );

      // Batch updatedProfileIds array into batches of 200 ids
      const batches = chunkArray(updatedProfileIds, 200);

      // Send each batch to SQS
      for (const batch of batches) {
        await sendMessagesToSQS(batch);
      }
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
