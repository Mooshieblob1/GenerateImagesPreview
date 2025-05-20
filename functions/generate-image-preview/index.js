import sharp from 'sharp';
import fetch from 'node-fetch';
import FormData from 'form-data';

const APPWRITE_ENDPOINT = 'https://syd.cloud.appwrite.io/v1';
const PROJECT_ID = '682b826b003d9cba9018';
const SOURCE_BUCKET_ID = '682b8a3a001fb3d3e9f2'; // Source bucket for original images
const TARGET_BUCKET_ID = 'your-target-bucket-id'; // Target bucket for WebP images
const DB_ID = '682b89cc0016319fcf30';
const SOURCE_COLLECTION_ID = '682b8a1a003b15611710'; // Source collection
const TARGET_COLLECTION_ID = 'your-target-collection-id'; // Target collection for WebP metadata

const API_KEY = process.env.APIWRITE_API_KEY;
const HEADERS = {
  'X-Appwrite-Project': PROJECT_ID,
  'X-Appwrite-Key': API_KEY,
};

export default async ({ req, res, log }) => {
  log('🔁 Starting image conversion to WebP');

  // Fetch documents from source collection
  const docsRes = await fetch(
    `${APPWRITE_ENDPOINT}/databases/${DB_ID}/collections/${SOURCE_COLLECTION_ID}/documents?limit=100`,
    { headers: HEADERS }
  );
  const { documents = [] } = await docsRes.json();

  // Fetch documents from target collection to check what's already processed
  const targetDocsRes = await fetch(
    `${APPWRITE_ENDPOINT}/databases/${DB_ID}/collections/${TARGET_COLLECTION_ID}/documents?limit=100`,
    { headers: HEADERS }
  );
  const { documents: targetDocs = [] } = await targetDocsRes.json();
  const processedIds = targetDocs.map((doc) => doc.originalImageId);

  for (const doc of documents) {
    // Skip if already processed
    if (processedIds.includes(doc.imageId)) continue;

    try {
      // Generate WebP and upload to target bucket
      const webpImageId = await generateAndUploadWebP(
        doc.imageId,
        doc.prompt,
        log
      );

      // Create a new document in target collection
      const webpDocData = {
        originalImageId: doc.imageId,
        webpImageId: webpImageId,
        prompt: doc.prompt,
        model: doc.model,
        tags: doc.tags,
        createdAt: new Date().toISOString(),
        originalCreatedAt: doc.createdAt,
      };

      const insertRes = await fetch(
        `${APPWRITE_ENDPOINT}/databases/${DB_ID}/collections/${TARGET_COLLECTION_ID}/documents`,
        {
          method: 'POST',
          headers: {
            ...HEADERS,
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({ documentId: 'unique()', data: webpDocData }),
        }
      );

      if (!insertRes.ok) {
        const err = await insertRes.text();
        log(`❌ Failed to insert WebP document: ${err}`);
      } else {
        log(`✅ Processed image ${doc.imageId} to WebP format`);
      }
    } catch (err) {
      log(`🔥 Error processing image ${doc.imageId}: ${err.message}`);
    }
  }

  log('✅ WebP conversion complete');
  return res.empty();
};

async function generateAndUploadWebP(fileId, fileName, log) {
  const fileUrl = `${APPWRITE_ENDPOINT}/storage/buckets/${SOURCE_BUCKET_ID}/files/${fileId}/download?project=${PROJECT_ID}`;
  const originalRes = await fetch(fileUrl, { headers: HEADERS });
  const imageBuffer = Buffer.from(await originalRes.arrayBuffer());

  const webpBuffer = await sharp(imageBuffer)
    .resize({ width: 480 })
    .webp({ quality: 75 })
    .toBuffer();

  const webpFileName = `webp-${fileId}.webp`;
  const form = new FormData();
  form.append('file', webpBuffer, webpFileName);
  form.append('fileId', 'unique()');
  form.append('name', webpFileName);

  const uploadRes = await fetch(
    `${APPWRITE_ENDPOINT}/storage/buckets/${TARGET_BUCKET_ID}/files`,
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

  const webpFile = await uploadRes.json();
  log(`🖼️ Created WebP version ${webpFile.$id} for ${fileName}`);
  return webpFile.$id;
}
