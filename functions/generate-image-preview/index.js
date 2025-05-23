import sharp from 'sharp';
import fetch from 'node-fetch';
import FormData from 'form-data';
import { Client, Databases, Query } from 'node-appwrite';

const APPWRITE_ENDPOINT = 'https://syd.cloud.appwrite.io/v1';
const PROJECT_ID = '682b826b003d9cba9018';
const SOURCE_BUCKET_ID = '682b8a3a001fb3d3e9f2';
const TARGET_BUCKET_ID = '682cfa1a0016991596f5';
const DB_ID = '682b89cc0016319fcf30';
const SOURCE_COLLECTION_ID = '682b8a1a003b15611710';
const TARGET_COLLECTION_ID = '682cf95a00397776afa6';

const API_KEY = process.env.APIWRITE_API_KEY;

export default async ({ req, res, log }) => {
  log('🔁 Starting image conversion and cleanup');

  const client = new Client()
    .setEndpoint(APPWRITE_ENDPOINT)
    .setProject(PROJECT_ID)
    .setKey(API_KEY);

  const databases = new Databases(client);

  const sourceDocuments = await paginateDocuments(
    databases,
    SOURCE_COLLECTION_ID,
    log
  );
  const webpDocuments = await paginateDocuments(
    databases,
    TARGET_COLLECTION_ID,
    log
  );

  const sourceImageIds = new Set(sourceDocuments.map((doc) => doc.imageId));

  let skipped = 0;
  let converted = 0;
  let cleaned = 0;
  let alreadyProcessed = 0;
  let payloadTooLarge = 0;
  let failedInserts = 0;

  const existingProcessed = new Set();
  const orphanedMetadata = [];

  for (const doc of webpDocuments) {
    const headRes = await fetch(
      `${APPWRITE_ENDPOINT}/storage/buckets/${TARGET_BUCKET_ID}/files/${doc.webpImageId}/view`,
      { method: 'HEAD', headers: getHeaders() }
    );

    const hasWebp = headRes.ok;
    const hasSource = sourceImageIds.has(doc.originalImageId);

    if (hasWebp && hasSource) {
      existingProcessed.add(doc.originalImageId);
    } else {
      log(
        `🧹 Cleaning up orphaned or stale doc ${doc.$id} (${doc.originalImageId})`
      );
      orphanedMetadata.push(doc);
    }
  }

  for (const doc of orphanedMetadata) {
    await fetch(
      `${APPWRITE_ENDPOINT}/databases/${DB_ID}/collections/${TARGET_COLLECTION_ID}/documents/${doc.$id}`,
      {
        method: 'DELETE',
        headers: getHeaders(),
      }
    );
    await fetch(
      `${APPWRITE_ENDPOINT}/storage/buckets/${TARGET_BUCKET_ID}/files/${doc.webpImageId}`,
      {
        method: 'DELETE',
        headers: getHeaders(),
      }
    );
    cleaned++;
  }

  for (const doc of sourceDocuments) {
    if (existingProcessed.has(doc.imageId)) {
      alreadyProcessed++;
      log(`⏭️ Skipping ${doc.imageId}: already processed`);
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
      };

      const payload = {
        documentId: 'unique()',
        ...webpDocData,
      };

      const rawBody = JSON.stringify(payload);
      const sizeKB = Buffer.byteLength(rawBody) / 1024;
      log(`📏 Payload size for ${doc.imageId}: ${sizeKB.toFixed(2)} KB`);

      if (sizeKB > 16000) {
        log(
          `⚠️ Skipping ${doc.imageId}: payload too large (${sizeKB.toFixed(
            2
          )} KB)`
        );
        payloadTooLarge++;
        continue;
      }

      const insertRes = await fetch(
        `${APPWRITE_ENDPOINT}/databases/${DB_ID}/collections/${TARGET_COLLECTION_ID}/documents?documentId=unique()`,
        {
          method: 'POST',
          headers: {
            ...getHeaders(),
            'Content-Type': 'application/json',
          },
          body: rawBody,
        }
      );

      if (!insertRes.ok) {
        const err = await insertRes.text();
        log(`❌ Failed to insert WebP metadata for ${doc.imageId}: ${err}`);
        failedInserts++;
      } else {
        log(`✅ Processed image ${doc.imageId} to WebP format`);
        converted++;
      }
    } catch (err) {
      log(`🔥 Error processing image ${doc.imageId}: ${err.message}`);
    }
  }

  log('✅ Image processing and cleanup complete');
  log('🔢 Summary:');
  log(`Total source: ${sourceDocuments.length}`);
  log(`Already processed: ${alreadyProcessed}`);
  log(`Too large to insert: ${payloadTooLarge}`);
  log(`Failed inserts: ${failedInserts}`);
  log(`Successfully inserted: ${converted}`);
  log(`Cleaned: ${cleaned}`);
  log(`Skipped (manual skips): ${skipped}`);

  return res.json({
    success: true,
    converted,
    skipped,
    cleaned,
    alreadyProcessed,
    payloadTooLarge,
    failedInserts,
    totalSourceImages: sourceDocuments.length,
    totalMetadata: webpDocuments.length,
  });
};

async function paginateDocuments(databases, collectionId, log) {
  let allDocs = [];
  let cursor = null;

  while (true) {
    const queries = [Query.limit(500)];
    if (cursor) queries.push(Query.cursorAfter(cursor));

    log(`📡 About to fetch documents from: ${collectionId}`);
    log(`📡 Queries: ${JSON.stringify(queries)}`);

    const result = await databases.listDocuments(DB_ID, collectionId, queries);
    const documents = result.documents;

    log(`📡 Fetched ${documents.length} from ${collectionId}`);
    if (documents.length === 0) break;

    allDocs.push(...documents);
    if (documents.length < 500) break;

    cursor = documents[documents.length - 1].$id;
  }

  return allDocs;
}

function getHeaders() {
  return {
    'X-Appwrite-Project': PROJECT_ID,
    'X-Appwrite-Key': API_KEY,
  };
}

async function generateAndUploadWebP(fileId, fileName, log) {
  const fileUrl = `${APPWRITE_ENDPOINT}/storage/buckets/${SOURCE_BUCKET_ID}/files/${fileId}/download?project=${PROJECT_ID}`;
  const originalRes = await fetch(fileUrl, { headers: getHeaders() });
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
        ...getHeaders(),
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
