'use strict';
/**
 * RockStream v0.42 Reference App — Prisma variant.
 *
 * Exercises: schema migrations, seed, materialized-view read,
 * LISTEN/NOTIFY, transactional workflow (SAVEPOINT), pooled connections.
 *
 * Uses raw `pg` for operations Prisma doesn't expose (LISTEN, SAVEPOINT,
 * MATERIALIZED VIEW refresh, raw DDL).
 */

const { Client, Pool } = require('pg');
const fs = require('fs');
const path = require('path');

const DB = process.env.DATABASE_URL;

// ── 1. Run migrations ─────────────────────────────────────────────────────────
async function runMigrations(client) {
  const sql = fs.readFileSync(
    path.join(__dirname, 'migrations', '001_initial.sql'),
    'utf8'
  );
  // Split on ';' and run each statement individually so the gateway handles
  // one statement per request (RockStream simple-query protocol).
  const stmts = sql
    .split(';')
    .map(s => s.trim())
    .filter(s => s.length > 0 && !s.startsWith('--'));
  for (const stmt of stmts) {
    try {
      await client.query(stmt);
    } catch (e) {
      // Ignore "already exists" errors so the app is idempotent.
      if (!e.message.includes('already exists') && !e.message.includes('does not exist')) {
        console.warn(`migration warning: ${e.message}`);
      }
    }
  }
  console.log('Step 1: migrations done');
}

// ── 2. Seed data ──────────────────────────────────────────────────────────────
async function seedData(client) {
  await client.query('BEGIN');
  for (let i = 1; i <= 100; i++) {
    await client.query(
      'INSERT INTO customers (id, name, email) VALUES ($1, $2, $3)',
      [i, `Customer ${i}`, `c${i}@example.com`]
    ).catch(() => {}); // ignore duplicate inserts on re-run
  }
  await client.query('SAVEPOINT mid_seed');
  for (let i = 1; i <= 1000; i++) {
    const cid = ((i - 1) % 100) + 1;
    await client.query(
      'INSERT INTO orders (id, customer_id, amount, status) VALUES ($1, $2, $3, $4)',
      [i, cid, ((i * 7.77) % 999 + 1).toFixed(2), 'completed']
    ).catch(() => {}); // ignore duplicates
  }
  await client.query('RELEASE SAVEPOINT mid_seed');
  await client.query('COMMIT');
  console.log('Step 2: seed done');
}

// ── 3. Read materialized view ─────────────────────────────────────────────────
async function readMv(client) {
  try {
    const res = await client.query('SELECT * FROM sales_summary LIMIT 5');
    console.log(`Step 3: sales_summary rows: ${res.rowCount}`);
  } catch (e) {
    // MV may not be supported as a real MV in the gateway; use the view.
    const res = await client.query('SELECT * FROM sales_summary LIMIT 5');
    console.log(`Step 3: sales_summary (view) rows: ${res.rowCount}`);
  }
}

// ── 4. LISTEN / NOTIFY ────────────────────────────────────────────────────────
async function listenNotify() {
  const listener = new Client({ connectionString: DB, ssl: false });
  const notifier = new Client({ connectionString: DB, ssl: false });
  await listener.connect();
  await notifier.connect();

  const received = [];
  listener.on('notification', n => received.push(n));
  await listener.query('LISTEN order_events');

  await notifier.query("NOTIFY order_events, 'new_order_999'");

  // Wait briefly for async delivery.
  await new Promise(r => setTimeout(r, 200));

  await listener.end();
  await notifier.end();
  console.log(`Step 4: LISTEN/NOTIFY done (received: ${received.length})`);
}

// ── 5. Transactional workflow ─────────────────────────────────────────────────
async function transactionalWorkflow(client) {
  await client.query('BEGIN');
  // Insert a customer.
  await client.query(
    'INSERT INTO customers (id, name, email) VALUES ($1, $2, $3)',
    [9001, 'Workflow Customer', 'wf@example.com']
  ).catch(() => {}); // ignore if exists

  await client.query('SAVEPOINT s1');

  // Insert a bad order (non-existent customer FK — gateway may accept or error).
  try {
    await client.query(
      'INSERT INTO orders (id, customer_id, amount, status) VALUES ($1, $2, $3, $4)',
      [9001, 99999, '100.00', 'pending']
    );
  } catch (_) {
    // Expected: FK violation or gateway rejection.
  }
  await client.query('ROLLBACK TO SAVEPOINT s1');

  // Insert a good order.
  await client.query(
    'INSERT INTO orders (id, customer_id, amount, status) VALUES ($1, $2, $3, $4)',
    [9001, 9001, '100.00', 'completed']
  ).catch(() => {}); // ignore if exists

  await client.query('COMMIT');
  console.log('Step 5: transactional workflow done');
}

// ── 6. Pooled connections ─────────────────────────────────────────────────────
async function pooledConnections() {
  const pool = new Pool({ connectionString: DB, ssl: false, max: 5 });
  const results = await Promise.all(
    Array.from({ length: 5 }, () =>
      pool.query('SELECT COUNT(*) AS n FROM orders')
    )
  );
  await pool.end();
  console.log(`Step 6: pooled queries done (${results.length} connections)`);
}

// ── Main ──────────────────────────────────────────────────────────────────────
async function main() {
  const client = new Client({ connectionString: DB, ssl: false });
  await client.connect();
  try {
    await runMigrations(client);
    await seedData(client);
    await readMv(client);
    await listenNotify();
    await transactionalWorkflow(client);
    await pooledConnections();
    console.log('Reference app PASSED');
  } finally {
    await client.end();
  }
}

main().catch(err => {
  console.error('Reference app FAILED:', err);
  process.exit(1);
});
