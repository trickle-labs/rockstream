'use strict';
const { Client } = require('pg');

const DB = process.env.DATABASE_URL;

async function seed(client) {
  // Seed 100 customers and 1000 orders with a SAVEPOINT mid-batch.
  await client.query('BEGIN');

  for (let i = 1; i <= 100; i++) {
    await client.query(
      'INSERT INTO customers (id, name, email) VALUES ($1, $2, $3)',
      [i, `Customer ${i}`, `customer${i}@example.com`]
    );
  }

  await client.query('SAVEPOINT mid_seed');

  for (let i = 1; i <= 1000; i++) {
    const customerId = ((i - 1) % 100) + 1;
    const amount = (Math.random() * 999 + 1).toFixed(2);
    await client.query(
      'INSERT INTO orders (id, customer_id, amount, status) VALUES ($1, $2, $3, $4)',
      [i, customerId, amount, 'completed']
    );
  }

  await client.query('RELEASE SAVEPOINT mid_seed');
  await client.query('COMMIT');
  console.log('Seed complete: 100 customers, 1000 orders');
}

async function main() {
  const client = new Client({ connectionString: DB, ssl: false });
  await client.connect();
  try {
    await seed(client);
  } finally {
    await client.end();
  }
}

main().catch(err => { console.error('seed error:', err); process.exit(1); });
