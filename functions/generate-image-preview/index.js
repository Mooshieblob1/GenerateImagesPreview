import sharp from 'sharp';
import fetch from 'node-fetch';
import FormData from 'form-data';

const APPWRITE_ENDPOINT = 'https://syd.cloud.appwrite.io/v1';
const PROJECT_ID = '682b826b003d9cba9018';
const SOURCE_BUCKET_ID = '682b8a3a001fb3d3e9f2';
const TARGET_BUCKET_ID = '682cfa1a0016991596f5';
const DB_ID = '682b89cc0016319fcf30';
const SOURCE_COLLECTION_ID = '682b8a1a003b15611710';
const TARGET_COLLECTION_ID = '682cf95a00397776afa6';

const API_KEY = process.env.APIWRITE_API_KEY;
const HEADERS = {
  'X-Appwrite-Project': PROJECT_ID,
  'X-Appwrite-Key': API_KEY,
};

export default async ({ req, res, log }) => {
  log('🔁 Starting image conversion and cleanup');

  // Step 1: Fetch source images and build a Set
  const sourceDocsRes = await fetch(
    `${APPWRITE_ENDPOINT}/databases/${DB_ID}/collections/${SOURCE_COLLECTION_ID}/documents?limit=100`,
    { headers: HEADERS }
  );
  const { documents: sourceDocuments = [] } = await sourceDocsRes.json();
  const sourceImageIds = new Set(sourceDocuments.map((doc) => doc.imageId));

  // Step 2: Fetch existing WebP metadata
  const webpDocsRes = await fetch(
    `${APPWRITE_ENDPOINT}/databases/${DB_ID}/collections/${TARGET_COLLECTION_ID}/documents?limit=100`,
    { headers: HEADERS }
  );
  const { documents: webpDocuments = [] } = await webpDocsRes.json();

  let skipped = 0;
  let converted = 0;
  let cleaned = 0;

  // Step 3: Clean up metadata and files for missing originals
  for (const doc of webpDocuments) {
    if (!sourceImageIds.has(doc.originalImageId)) {
      log(
        `🧹 Cleaning up metadata + WebP for missing source image ${doc.originalImageId}`
      );

      // Delete metadata
      await fetch(
        `${APPWRITE_ENDPOINT}/databases/${DB_ID}/collections/${TARGET_COLLECTION_ID}/documents/${doc.$id}`,
        {
          method: 'DELETE',
          headers: HEADERS,
        }
      );

      // Delete WebP file
      await fetch(
        `${APPWRITE_ENDPOINT}/storage/buckets/${TARGET_BUCKET_ID}/files/${doc.webpImageId}`,
        {
          method: 'DELETE',
          headers: HEADERS,
        }
      );

      cleaned++;
    }
  }

  // Step 4: Generate WebP for unprocessed images
  const existingProcessed = new Set(
    webpDocuments.map((doc) => doc.originalImageId)
  );

  for (const doc of sourceDocuments) {
    if (existingProcessed.has(doc.imageId)) {
      skipped++;
      continue;
    }

    try {
      const webpImageId = await generateAndUploadWebP(
        doc.imageId,
        doc.prompt,
        log
      );

      const webpDocData = {
        originalImageId: doc.imageId,
        webpImageId,
        prompt: doc.prompt,
        model: doc.model,
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
        log(`❌ Failed to insert WebP metadata: ${err}`);
      } else {
        log(`✅ Processed image ${doc.imageId} to WebP format`);
        converted++;
      }
    } catch (err) {
      log(`🔥 Error processing image ${doc.imageId}: ${err.message}`);
    }
  }

  log('✅ Image processing and cleanup complete');
  return res.json({
    success: true,
    converted,
    skipped,
    cleaned,
    totalSourceImages: sourceDocuments.length,
    totalMetadata: webpDocuments.length,
  });
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
