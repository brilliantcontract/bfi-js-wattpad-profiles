import dotenv from "dotenv";
import he from "he";
import { JSDOM } from "jsdom";
import { Client } from "pg";

dotenv.config();

const SCRAPE_NINJA_ENDPOINT = "https://scrapeninja.p.rapidapi.com/scrape";
const SCRAPE_NINJA_HOST = "scrapeninja.p.rapidapi.com";
const DEFAULT_SCRAPE_NINJA_API_KEY =
  "455e2a6556msheffc310f7420b51p102ea0jsn1c531be1e299";

const DB_CONFIG = {
  host: process.env.DB_HOST || "3.140.167.34",
  port: Number.parseInt(process.env.DB_PORT, 10) || 5432,
  user: process.env.DB_USER || "redash",
  password: process.env.DB_PASSWORD || "te83NECug38ueP",
  database: process.env.DB_NAME || "scrapers",
};

const REQUEST_HEADERS = {
  accept:
    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
  "accept-language": "en-US,en;q=0.9",
  "cache-control": "max-age=0",
  cookie:
    "sn__time=j%3Anull; fs__exp=1; adMetrics=0; wp-web-auth-cache-bust=0; _pbeb_=1; _afp25f_=0; _uaef25_=1; lang=1; locale=en_US; wp_id=449600db-dd66-4e3f-af8a-6c1ff55afabf; ff=1; dpr=1; tz=-2; X-Time-Zone=Europe%2FKiev; te_session_id=1764965146161; signupFrom=user_profile; _col_uuid=21c1edd6-628b-4fac-923d-76911bb4d0ed-4y7yg; AMP_TOKEN=%24NOT_FOUND; _gid=GA1.2.857101283.1764965147; _fbp=fb.1.1764965147342.648629939101461537; _gcl_au=1.1.960895685.1764965147; _ga_FNDTZ0MZDQ=GS2.1.s1764965146$o1$g1$t1764965519$j57$l0$h0; _ga=GA1.1.675412621.1764965147; g_state={\"i_l\":0,\"i_ll\":1764965521223,\"i_b\":\"eKcNSzeTata8N0kVg8HjDlt6TeicHHQQ5zm4UnIVEtc\"}; RT=r=https%3A%2F%2Fwww.wattpad.com%2Fuser%2FCutiePie_loveOt7&ul=1764967111649",
  "if-none-match": "W/\"21067-R6nGiB9rkovGLyFuTXwX8alXLOw\"",
  priority: "u=0, i",
  "sec-ch-ua": '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
  "sec-ch-ua-mobile": "?0",
  "sec-ch-ua-platform": '"Windows"',
  "sec-fetch-dest": "document",
  "sec-fetch-mode": "navigate",
  "sec-fetch-site": "none",
  "sec-fetch-user": "?1",
  "upgrade-insecure-requests": "1",
  "user-agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
};

const PROFILE_INSERT_QUERY = `
  INSERT INTO wattpad.profiles (
    profile_image_url,
    profile_name,
    username,
    first_name,
    last_name,
    verified,
    number_of_work,
    number_of_readling_list,
    number_of_followers,
    description,
    gender,
    location,
    join_date,
    facebook_link,
    other_link,
    number_following
  ) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
  );
`;

const PROFILE_UPDATE_QUERY = `
  UPDATE wattpad.profiles
  SET
    profile_image_url = $1,
    profile_name = $2,
    first_name = $4,
    last_name = $5,
    verified = $6,
    number_of_work = $7,
    number_of_readling_list = $8,
    number_of_followers = $9,
    description = $10,
    gender = $11,
    location = $12,
    join_date = $13,
    facebook_link = $14,
    other_link = $15,
    number_following = $16
  WHERE username = $3;
`;

const fetchFn = globalThis.fetch;

function coerceToString(value) {
  if (value === undefined || value === null) return null;
  return String(value);
}

function truncateString(value, maxLength) {
  const stringValue = coerceToString(value);
  if (stringValue === null) return null;
  if (stringValue.length > maxLength) {
    return stringValue.slice(0, maxLength);
  }
  return stringValue;
}

function balanceBraces(text, startIndex) {
  let depth = 0;
  let inString = false;
  let escaped = false;
  const start = text.indexOf("{", startIndex);
  if (start === -1) return null;

  for (let i = start; i < text.length; i += 1) {
    const char = text[i];
    if (escaped) {
      escaped = false;
      continue;
    }
    if (char === "\\") {
      escaped = true;
      continue;
    }
    if (char === '"') {
      inString = !inString;
    }
    if (inString) continue;
    if (char === "{") depth += 1;
    if (char === "}") depth -= 1;
    if (depth === 0) {
      return text.slice(start, i + 1);
    }
  }
  return null;
}

function parseJsonFromScript(content) {
  const trimmed = content.trim();
  if (!trimmed) return null;
  const candidates = [];
  try {
    candidates.push(JSON.parse(trimmed));
  } catch (error) {
    const balanced = balanceBraces(trimmed, 0);
    if (balanced) {
      try {
        candidates.push(JSON.parse(balanced));
      } catch (innerError) {
        // continue searching for nested JSON
      }
    }
  }
  return candidates.find((candidate) => candidate !== null) || null;
}

function findProfileDataCandidate(value) {
  if (!value || typeof value !== "object") return null;
  if (Array.isArray(value)) {
    for (const item of value) {
      const found = findProfileDataCandidate(item);
      if (found) return found;
    }
    return null;
  }

  const profileKeys = [
    "avatar",
    "username",
    "name",
    "firstName",
    "lastName",
    "numStoriesPublished",
    "numFollowers",
  ];
  if (profileKeys.some((key) => key in value)) {
    return value;
  }
  for (const key of Object.keys(value)) {
    const found = findProfileDataCandidate(value[key]);
    if (found) return found;
  }
  return null;
}

function extractDataFromDocument(document, rawHtml) {
  const scripts = Array.from(document.querySelectorAll("script"));
  for (const script of scripts) {
    const content = script.textContent || "";
    const parsed = parseJsonFromScript(content);
    if (parsed) {
      const candidate = findProfileDataCandidate(parsed);
      if (candidate) return candidate;
    }
    const balancedText = balanceBraces(content, content.indexOf('"data"'));
    if (balancedText) {
      try {
        const parsedBalanced = JSON.parse(balancedText);
        const candidate = findProfileDataCandidate(parsedBalanced);
        if (candidate) return candidate;
      } catch (error) {
        // ignore parsing errors
      }
    }
  }

  const fallbackBalanced = balanceBraces(rawHtml, rawHtml.indexOf('"data"'));
  if (fallbackBalanced) {
    try {
      const parsedFallback = JSON.parse(fallbackBalanced);
      const candidate = findProfileDataCandidate(parsedFallback);
      if (candidate) return candidate;
    } catch (error) {
      // ignore parsing errors
    }
  }

  return null;
}

function buildProfileRecord(username, data) {
  return {
    profile_image_url: truncateString(data?.avatar, 4000),
    profile_name: truncateString(data?.name, 4000),
    username: truncateString(data?.username || username, 4000),
    first_name: truncateString(data?.firstName, 4000),
    last_name: truncateString(data?.lastName, 4000),
    verified: truncateString(data?.verified, 4000),
    number_of_work: truncateString(data?.numStoriesPublished, 4000),
    number_of_readling_list: truncateString(data?.numLists, 4000),
    number_of_followers: truncateString(data?.numFollowers, 4000),
    description:
      truncateString(he.decode(data?.description || ""), 4000) || null,
    gender: truncateString(data?.gender, 4000),
    location: truncateString(data?.location, 4000),
    join_date: truncateString(data?.createDate, 4000),
    facebook_link: truncateString(data?.facebook, 4000),
    other_link: truncateString(data?.website, 4000),
    number_following: truncateString(data?.numFollowing, 4000),
  };
}

async function fetchProfileHtml(username, apiKey) {
  if (!fetchFn) {
    throw new Error("Fetch API is not available in this environment.");
  }

  const profileUrl = `https://www.wattpad.com/user/${encodeURIComponent(
    username.trim()
  )}`;

  // Attempt a direct fetch first to avoid proxy-related validation errors (e.g. 422)
  const directResponse = await fetchFn(profileUrl, {
    headers: REQUEST_HEADERS,
  });

  if (directResponse.ok) {
    return directResponse.text();
  }

  const response = await fetchFn(SCRAPE_NINJA_ENDPOINT, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-RapidAPI-Key": apiKey || DEFAULT_SCRAPE_NINJA_API_KEY,
      "X-RapidAPI-Host": SCRAPE_NINJA_HOST,
    },
    body: JSON.stringify({
      url: profileUrl,
      headers: REQUEST_HEADERS,
    }),
  });

  if (!response.ok) {
    const errorBody = await response.text();
    throw new Error(
      `Failed to fetch profile for ${username}: ${response.status} ${errorBody}`
    );
  }

  const payload = await response.json();
  const html = payload.body || payload.html || payload.result || "";
  if (!html) {
    throw new Error(`Empty HTML returned for ${username}`);
  }
  return html;
}

async function selectUsernames(client) {
  const result = await client.query(
    "SELECT username FROM wattpad.not_scraped_searches_vw"
  );
  return result.rows.map((row) => row.username).filter(Boolean);
}

async function saveProfile(client, record) {
  const values = [
    record.profile_image_url,
    record.profile_name,
    record.username,
    record.first_name,
    record.last_name,
    record.verified,
    record.number_of_work,
    record.number_of_readling_list,
    record.number_of_followers,
    record.description,
    record.gender,
    record.location,
    record.join_date,
    record.facebook_link,
    record.other_link,
    record.number_following,
  ];

  await client.query("BEGIN");
  try {
    const updateResult = await client.query(PROFILE_UPDATE_QUERY, values);
    if (updateResult.rowCount === 0) {
      await client.query(PROFILE_INSERT_QUERY, values);
    }
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  }
}

async function processUsername(client, username, apiKey) {
  const html = await fetchProfileHtml(username, apiKey);
  const dom = new JSDOM(html);
  const data = extractDataFromDocument(dom.window.document, html);

  if (!data) {
    throw new Error(`Unable to locate profile data for ${username}`);
  }

  const record = buildProfileRecord(username, data);
  await saveProfile(client, record);
  return record;
}

async function scrapeProfiles() {
  const client = new Client(DB_CONFIG);
  await client.connect();
  try {
    const usernames = await selectUsernames(client);
    const apiKey = process.env.SCRAPE_NINJA_API_KEY;

    for (const username of usernames) {
      try {
        console.log(`Processing ${username}...`);
        await processUsername(client, username, apiKey);
        console.log(`Saved profile for ${username}`);
      } catch (error) {
        console.error(`Error processing ${username}:`, error.message);
      }
    }
  } finally {
    await client.end();
  }
}

if (process.argv[2] === "scrape" || process.argv.length === 2) {
  scrapeProfiles().catch((error) => {
    console.error("Fatal error during scraping:", error);
    process.exitCode = 1;
  });
}
