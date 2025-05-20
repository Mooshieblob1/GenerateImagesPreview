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
  log('🔁 Starting image conversion to WebP');

  const docsRes = await fetch(
    `${APPWRITE_ENDPOINT}/databases/${DB_ID}/collections/${SOURCE_COLLECTION_ID}/documents?limit=100`,
    { headers: HEADERS }
  );
  const { documents = [] } = await docsRes.json();

  const targetDocsRes = await fetch(
    `${APPWRITE_ENDPOINT}/databases/${DB_ID}/collections/${TARGET_COLLECTION_ID}/documents?limit=100`,
    { headers: HEADERS }
  );
  const { documents: targetDocs = [] } = await targetDocsRes.json();
  const processedIds = targetDocs.map((doc) => doc.originalImageId);

  let skipped = 0;
  let converted = 0;

  for (const doc of documents) {
    if (processedIds.includes(doc.imageId)) {
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
        webpImageId: webpImageId,
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
        log(`❌ Failed to insert WebP document: ${err}`);
      } else {
        log(`✅ Processed image ${doc.imageId} to WebP format`);
        converted++;
      }
    } catch (err) {
      log(`🔥 Error processing image ${doc.imageId}: ${err.message}`);
    }
  }

  log('✅ WebP conversion complete');
  return res.json({
    success: true,
    converted,
    skipped,
    total: documents.length,
    message: 'WebP conversion run completed.',
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
