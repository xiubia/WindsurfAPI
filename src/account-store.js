import { Pool } from 'pg';
import { config, log } from './config.js';

const pool = config.databaseUrl
  ? new Pool({
      connectionString: config.databaseUrl,
      ssl: config.databaseSsl ? { rejectUnauthorized: false } : undefined,
      max: 3,
      idleTimeoutMillis: 30000,
    })
  : null;

let initPromise = null;

function rowToAccount(row) {
  return {
    id: row.id,
    email: row.email,
    apiKey: row.api_key,
    apiServerUrl: row.api_server_url || '',
    method: row.method || 'api_key',
    status: row.status || 'active',
    addedAt: Number(row.added_at) || Date.now(),
    tier: row.tier || 'unknown',
    tierManual: !!row.tier_manual,
    capabilities: row.capabilities || {},
    lastProbed: Number(row.last_probed) || 0,
    credits: row.credits || null,
    blockedModels: Array.isArray(row.blocked_models) ? row.blocked_models : [],
    refreshToken: row.refresh_token || '',
    userStatus: row.user_status || null,
    userStatusLastFetched: Number(row.user_status_last_fetched) || 0,
  };
}

function accountToParams(account) {
  return [
    account.id,
    account.email,
    account.apiKey,
    account.apiServerUrl || '',
    account.method || 'api_key',
    account.status || 'active',
    Number(account.addedAt) || Date.now(),
    account.tier || 'unknown',
    !!account.tierManual,
    account.capabilities || {},
    Number(account.lastProbed) || 0,
    account.credits || null,
    Array.isArray(account.blockedModels) ? account.blockedModels : [],
    account.refreshToken || '',
    account.userStatus || null,
    Number(account.userStatusLastFetched) || 0,
  ];
}

async function init() {
  if (!pool) return;
  if (!initPromise) {
    initPromise = pool.query(`
      CREATE TABLE IF NOT EXISTS windsurf_accounts (
        id TEXT PRIMARY KEY,
        email TEXT NOT NULL,
        api_key TEXT NOT NULL UNIQUE,
        api_server_url TEXT NOT NULL DEFAULT '',
        method TEXT NOT NULL DEFAULT 'api_key',
        status TEXT NOT NULL DEFAULT 'active',
        added_at BIGINT NOT NULL,
        tier TEXT NOT NULL DEFAULT 'unknown',
        tier_manual BOOLEAN NOT NULL DEFAULT FALSE,
        capabilities JSONB NOT NULL DEFAULT '{}'::jsonb,
        last_probed BIGINT NOT NULL DEFAULT 0,
        credits JSONB,
        blocked_models JSONB NOT NULL DEFAULT '[]'::jsonb,
        refresh_token TEXT NOT NULL DEFAULT '',
        user_status JSONB,
        user_status_last_fetched BIGINT NOT NULL DEFAULT 0
      )
    `).then(() => {
      log.info('Account store: PostgreSQL table ready');
    }).catch((err) => {
      initPromise = null;
      throw err;
    });
  }
  return initPromise;
}

export function isDatabaseAccountStoreEnabled() {
  return !!pool;
}

export async function loadAccountsFromDatabase() {
  if (!pool) return [];
  await init();
  const result = await pool.query('SELECT * FROM windsurf_accounts ORDER BY added_at ASC');
  return result.rows.map(rowToAccount);
}

export async function saveAccountsToDatabase(accounts) {
  if (!pool) return false;
  await init();
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query('DELETE FROM windsurf_accounts');
    for (const account of accounts) {
      const params = accountToParams(account);
      await client.query(`
        INSERT INTO windsurf_accounts (
          id, email, api_key, api_server_url, method, status, added_at,
          tier, tier_manual, capabilities, last_probed, credits,
          blocked_models, refresh_token, user_status, user_status_last_fetched
        ) VALUES (
          $1, $2, $3, $4, $5, $6, $7,
          $8, $9, $10, $11, $12,
          $13, $14, $15, $16
        )
      `, params);
    }
    await client.query('COMMIT');
    return true;
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}
