import fs from 'fs';
import path from 'path';
import fetch from 'node-fetch';
import { google } from 'googleapis';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const BASE_URL = 'https://api.jikan.moe/v4';
const MAX_PAGES = 1151;
const DELAY_MS = 1500;
const TEMP_OUTPUT_FILE = path.join(__dirname, 'characters.json');
const DRIVE_FOLDER_ID = process.env.DRIVE_FOLDER_ID;

const sleep = (ms) => new Promise((res) => setTimeout(res, ms));

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
  const serviceAccount = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT);

  const auth = new google.auth.GoogleAuth({
    credentials: serviceAccount,
    scopes: ['https://www.googleapis.com/auth/drive.file'],
  });

  const authClient = await auth.getClient();

  // To track duplicates without loading entire file:
  const existingNames = new Set();

  // Open file for streaming write
  const stream = fs.createWriteStream(TEMP_OUTPUT_FILE, { flags: 'w' });
  stream.write('['); // start JSON array

  let isFirst = true;

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

        // Add comma if not the first entry
        if (!isFirst) {
          stream.write(',\n');
        } else {
          isFirst = false;
        }

        stream.write(JSON.stringify(newChar));
        existingNames.add(char.name);

        await sleep(DELAY_MS);
      }
    } catch (err) {
      console.error(`❌ Failed to fetch page ${page}: ${err.message}`);
    }

    await sleep(DELAY_MS);
  }

  stream.write(']\n'); // end JSON array
  stream.end();

  console.log(`✅ Finished writing characters to ${TEMP_OUTPUT_FILE}`);

  await uploadToGoogleDrive(authClient, TEMP_OUTPUT_FILE, 'characters.json');
}

main();

