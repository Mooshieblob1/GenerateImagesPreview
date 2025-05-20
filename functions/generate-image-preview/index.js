import sharp from 'sharp';
import fetch from 'node-fetch';
import FormData from 'form-data';

const APPWRITE_ENDPOINT = 'https://syd.cloud.appwrite.io/v1';
const PROJECT_ID = '682b826b003d9cba9018';
const BUCKET_ID = '682b8a3a001fb3d3e9f2';
const DB_ID = '682b89cc0016319fcf30';
const COLLECTION_ID = '682b8a1a003b15611710';

const API_KEY = process.env.APIWRITE_API_KEY;
const HEADERS = {
  'X-Appwrite-Project': PROJECT_ID,
  'X-Appwrite-Key': API_KEY,
};

export default async ({ req, res, log }) => {
  log('🔁 Starting image preview sync');

  const filesRes = await fetch(
    `${APPWRITE_ENDPOINT}/storage/buckets/${BUCKET_ID}/files?limit=100`,
    { headers: HEADERS }
  );
  const { files = [] } = await filesRes.json();

  const docsRes = await fetch(
    `${APPWRITE_ENDPOINT}/databases/${DB_ID}/collections/${COLLECTION_ID}/documents?limit=100`,
    { headers: HEADERS }
  );
  const { documents = [] } = await docsRes.json();
  const existingIds = documents.map((doc) => doc.fullImageId);

  for (const file of files) {
    if (existingIds.includes(file.$id)) continue;

    const name = file.name.replace(/\.[^.]+$/, '');
    const [promptRaw, model = '', ...tagParts] = name.split('_');
    const tags = tagParts.join('_').split('+').filter(Boolean);
    const createdAt = new Date(file.$createdAt).toISOString();

    try {
      const previewImageId = await generateAndUploadWebP(
        file.$id,
        file.name,
        log
      );

      const docData = {
        prompt: decodeURIComponent(promptRaw),
        model,
        fullImageId: file.$id,
        previewImageId,
        tags,
        createdAt,
      };

      const insertRes = await fetch(
        `${APPWRITE_ENDPOINT}/databases/${DB_ID}/collections/${COLLECTION_ID}/documents`,
        {
          method: 'POST',
          headers: {
            ...HEADERS,
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({ documentId: 'unique()', data: docData }),
        }
      );

      if (!insertRes.ok) {
        const err = await insertRes.text();
        log(`❌ Failed to insert: ${err}`);
      } else {
        log(`✅ Inserted ${file.name} with preview`);
      }
    } catch (err) {
      log(`🔥 Error processing ${file.name}: ${err.message}`);
    }
  }

  log('✅ Sync complete');

  // ✅ Correct return here
  return res.empty();
};

async function generateAndUploadWebP(fileId, fileName, log) {
  const fileUrl = `${APPWRITE_ENDPOINT}/storage/buckets/${BUCKET_ID}/files/${fileId}/download?project=${PROJECT_ID}`;
  const originalRes = await fetch(fileUrl, { headers: HEADERS });
  const imageBuffer = await originalRes.buffer();

  const webpBuffer = await sharp(imageBuffer)
    .resize({ width: 480 })
    .webp({ quality: 75 })
    .toBuffer();

  const previewFileName = `preview-${fileId}.webp`;
  const form = new FormData();
  form.append('file', webpBuffer, previewFileName);
  form.append('name', previewFileName);

  const uploadRes = await fetch(
    `${APPWRITE_ENDPOINT}/storage/buckets/${BUCKET_ID}/files`,
    {
      method: 'POST',
      headers: {
        ...HEADERS,
        ...form.getHeaders(),
      },
      body: form,
    }
  );

  if (!uploadRes.ok) {
    const err = await uploadRes.text();
    throw new Error(`Failed to upload WebP: ${err}`);
  }

  const preview = await uploadRes.json();
  log(`🖼️ Created preview ${preview.$id} for ${fileName}`);
  return preview.$id;
}
