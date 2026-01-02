#!/usr/bin/env node
'use strict';

const path = require('path');
const os = require('os');
const puppeteer = require('puppeteer');

const USER_DATA_DIR = path.join(os.homedir(), '.config', 'hackerbook-github-browser');
const HEADLESS = process.env.HEADLESS !== 'false';

async function getTmateSSH(runId, repo) {
  const url = `https://github.com/${repo}/actions/runs/${runId}`;

  console.error('[Puppeteer] Launching browser...');
  const browser = await puppeteer.launch({
    headless: HEADLESS,
    userDataDir: USER_DATA_DIR,
    args: ['--no-sandbox', '--disable-setuid-sandbox']
  });

  try {
    const page = await browser.newPage();
    console.error(`[Puppeteer] Navigating to ${url}`);
    await page.goto(url, { waitUntil: 'networkidle2', timeout: 30000 });

    const needsLogin = await page.$('input[name="login"]');
    if (needsLogin) {
      console.error('[Puppeteer] ⚠️  Not logged in to GitHub!');

      if (!HEADLESS) {
        console.error('[Puppeteer] Browser window is open. Please login to GitHub.');
        console.error('[Puppeteer] Your login will be saved in:', USER_DATA_DIR);
        console.error('');
        console.error('👉 Press ENTER once you have logged in...');

        await new Promise((resolve) => {
          process.stdin.once('data', () => resolve());
        });

        console.error('[Puppeteer] Continuing...');
        await page.goto(url, { waitUntil: 'networkidle2', timeout: 30000 });
      } else {
        console.error('[Puppeteer] Run with HEADLESS=false to login');
        await browser.close();
        process.exit(1);
      }
    }

    console.error('[Puppeteer] Logged in, fetching job details...');
    await new Promise((resolve) => setTimeout(resolve, 3000));

    const jobs = await page.evaluate(() => {
      const jobLinks = Array.from(document.querySelectorAll('a[href*="/job/"]'));
      return jobLinks
        .map((link) => ({
          name: (link.textContent || '').trim() || 'Unknown',
          url: link.href,
          jobId: link.href.match(/\/job\/(\d+)/)?.[1]
        }))
        .filter((j) => j.jobId);
    });

    console.error(`[Puppeteer] Found ${jobs.length} jobs`);
    if (jobs.length === 0) {
      console.error('[Puppeteer] ⚠️  No jobs found (workflow may have just started)');
      await browser.close();
      process.exit(1);
    }

    const sshConnections = {};

    for (const job of jobs) {
      console.error(`[Puppeteer] Checking job: ${job.name}`);
      await page.goto(job.url, { waitUntil: 'networkidle2', timeout: 30000 });
      await new Promise((resolve) => setTimeout(resolve, 2000));

      const ssh = await page.evaluate(() => {
        const pageText = document.body.innerText;
        const sshMatch = pageText.match(/SSH:\s*(ssh\s+[^\s\n]+@[^\s\n]+)/);
        return sshMatch ? sshMatch[1] : null;
      });

      if (ssh) {
        console.error(`[Puppeteer] ✓ Found SSH: ${ssh}`);
        sshConnections[job.name] = ssh;
      } else {
        console.error(`[Puppeteer] ✗ No SSH in ${job.name} (tmate not started yet)`);
      }
    }

    console.log(JSON.stringify(sshConnections, null, 2));
    await browser.close();

    if (Object.keys(sshConnections).length === 0) {
      console.error('[Puppeteer] ⚠️  No SSH connections found (jobs still starting)');
      process.exit(1);
    }
  } catch (error) {
    console.error('[Puppeteer] ❌ Error:', error.message);
    await browser.close();
    process.exit(1);
  }
}

const runId = process.argv[2];
const repo = process.argv[3];

if (!runId || !repo) {
  console.error('Usage: get-tmate-ssh.js <run-id> <repo>');
  process.exit(1);
}

getTmateSSH(runId, repo);
