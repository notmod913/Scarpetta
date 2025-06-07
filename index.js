import fs from 'fs';
import path from 'path';
import fetch from 'node-fetch';
import { google } from 'googleapis';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';

// Load environment variables from .env
dotenv.config();

// Required for __dirname in ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Constants
const BASE_URL = 'https://api.jikan.moe/v4';
const MAX_PAGES = 1155;
const DELAY_MS = 1500;
const TEMP_OUTPUT_FILE = path.join(__dirname, 'characters.json');
const DRIVE_FOLDER_ID = process.env.DRIVE_FOLDER_ID;

// Sleep helper
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Upload to Google Drive
async function uploadToGoogleDrive(authClient, filePath, fileName) {
  const drive = google.drive({ version: 'v3', auth: authClient });

  const fileMetadata = {
    name: fileName,
    parents: [DRIVE_FOLDER_ID],
  };
  const media = {
    mimeType: 'application/json',
    body: fs.createReadStream(filePath),
  };

  try {
    const existingFiles = await drive.files.list({
      q: `'${DRIVE_FOLDER_ID}' in parents and name='${fileName}' and trashed=false`,
      fields: 'files(id, name)',
      spaces: 'drive',
    });

    if (existingFiles.data.files.length > 0) {
      const fileId = existingFiles.data.files[0].id;
      await drive.files.update({ fileId, media });
      console.log(`🔄 Updated existing file on Drive: ${fileName}`);
    } else {
      await drive.files.create({ resource: fileMetadata, media, fields: 'id' });
      console.log(`✅ Uploaded new file to Drive: ${fileName}`);
    }
  } catch (err) {
    console.error('❌ Failed to upload file to Google Drive:', err.message);
  }
}

async function main() {
  // Parse service account JSON from env variable
  let serviceAccount;
  try {
    serviceAccount = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT);
  } catch (err) {
    console.error('❌ Failed to parse GOOGLE_SERVICE_ACCOUNT JSON:', err.message);
    process.exit(1);
  }

  const auth = new google.auth.GoogleAuth({
    credentials: serviceAccount,
    scopes: ['https://www.googleapis.com/auth/drive.file'],
  });

  const authClient = await auth.getClient();

  // Load existing data
  let results = [];
  if (fs.existsSync(TEMP_OUTPUT_FILE)) {
    try {
      const raw = fs.readFileSync(TEMP_OUTPUT_FILE, 'utf-8').trim();
      results = raw ? JSON.parse(raw) : [];
      console.log(`📂 Loaded ${results.length} characters from local file`);
    } catch (err) {
      console.warn(`⚠️ Failed to parse local file: ${err.message}`);
    }
  }

  const existingNames = new Map(results.map((char) => [char.name, char]));

  for (let page = 1; page <= MAX_PAGES; page++) {
    console.log(`📄 Fetching character page ${page}`);

    try {
      const res = await fetch(`${BASE_URL}/characters?page=${page}`);
      const data = await res.json();

      for (const char of data.data) {
        if (existingNames.has(char.name)) {
          console.log(`🔁 Skipping duplicate: ${char.name}`);
          continue;
        }

        console.log(`👤 Fetching anime for: ${char.name}`);
        let anime_titles = [];

        try {
          const animeRes = await fetch(`${BASE_URL}/characters/${char.mal_id}/anime`);
          const animeData = await animeRes.json();
          anime_titles = animeData.data.map((entry) => entry.anime.title);
        } catch (err) {
          console.warn(`⚠️ Error fetching anime for ${char.name}: ${err.message}`);
        }

        const newChar = {
          name: char.name,
          image_url: char.images?.jpg?.image_url || null,
          anime_titles,
        };

        results.push(newChar);
        existingNames.set(char.name, newChar);

        await sleep(DELAY_MS);
      }
    } catch (err) {
      console.error(`❌ Failed to fetch page ${page}: ${err.message}`);
    }

    await sleep(DELAY_MS);
  }

  fs.writeFileSync(TEMP_OUTPUT_FILE, JSON.stringify(results, null, 2));
  console.log(`✅ Saved ${results.length} characters to local file`);

  await uploadToGoogleDrive(authClient, TEMP_OUTPUT_FILE, 'characters.json');
}

main();

