#!/usr/bin/env node
import { createServer } from 'http';
import { readFile, stat } from 'fs/promises';
import { join, extname } from 'path';
import { fileURLToPath } from 'url';

const PORT = process.env.PORT || 3000;
const __dirname = fileURLToPath(new URL('.', import.meta.url));
const DOCS_DIR = join(__dirname, 'docs');

const MIME_TYPES = {
  '.html': 'text/html',
  '.js': 'text/javascript',
  '.css': 'text/css',
  '.json': 'application/json',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.gif': 'image/gif',
  '.svg': 'image/svg+xml',
  '.wasm': 'application/wasm',
  '.gz': 'application/gzip',
  '.sqlite': 'application/octet-stream',
  '.bin': 'application/octet-stream',
};

const server = createServer(async (req, res) => {
  try {
    let filePath = join(DOCS_DIR, req.url === '/' ? 'index.html' : req.url);
    
    // Remove query strings
    const [pathOnly] = filePath.split('?');
    filePath = pathOnly;

    const stats = await stat(filePath);
    
    if (!stats.isFile()) {
      res.writeHead(404);
      res.end('Not Found');
      return;
    }

    const ext = extname(filePath).toLowerCase();
    const mimeType = MIME_TYPES[ext] || 'application/octet-stream';
    
    // Set cache headers based on file type
    const headers = { 'Content-Type': mimeType };
    const fileName = filePath.split('/').pop();
    
    // Check if this is a manifest/index file (used with ?v= cache busting)
    const isManifest = fileName.includes('manifest.json') || 
                       fileName.includes('-index.json') || 
                       fileName.includes('-index.bin');
    
    if (ext === '.html') {
      // No cache for HTML files
      headers['Cache-Control'] = 'no-cache, no-store, must-revalidate';
      headers['Pragma'] = 'no-cache';
      headers['Expires'] = '0';
    } else if (isManifest) {
      // 1 day cache with revalidation for manifests/indexes (they use ?v= cache busting)
      headers['Cache-Control'] = 'public, max-age=86400, must-revalidate';
    } else if ((ext === '.sqlite' || ext === '.gz') && filePath.includes('shard')) {
      // Long cache for shards (they have content hashes in filenames)
      headers['Cache-Control'] = 'public, max-age=31536000, immutable';
    } else {
      // Default: reasonable cache for other static files
      headers['Cache-Control'] = 'public, max-age=3600';
    }

    const content = await readFile(filePath);
    res.writeHead(200, headers);
    res.end(content);
    
    console.log(`${new Date().toLocaleTimeString()} ${req.method} ${req.url} - ${headers['Cache-Control']}`);
  } catch (err) {
    if (err.code === 'ENOENT') {
      res.writeHead(404);
      res.end('Not Found');
    } else {
      res.writeHead(500);
      res.end('Internal Server Error');
      console.error(err);
    }
  }
});

server.listen(PORT, () => {
  console.log(`\nServing docs/ on http://localhost:${PORT}`);
  console.log('HTML files: no-cache');
  console.log('Static assets (.sqlite, .gz, .bin): immutable, max-age=1yr');
  console.log('Other files: max-age=1hr\n');
});
