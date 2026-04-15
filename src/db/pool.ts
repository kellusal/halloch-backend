import 'dotenv/config';
import { Pool } from 'pg';

const connectionString = process.env.DATABASE_URL;

if (!connectionString) {
  throw new Error('DATABASE_URL is not configured');
}

export const pool = new Pool({
  connectionString,
});

pool.on('error', (err: Error) => {
  console.error('Unexpected PostgreSQL pool error:', err);
});
