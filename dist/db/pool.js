"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.pool = void 0;
require("dotenv/config");
const pg_1 = require("pg");
const connectionString = process.env.DATABASE_URL;
if (!connectionString) {
    throw new Error('DATABASE_URL is not configured');
}
exports.pool = new pg_1.Pool({
    connectionString,
});
exports.pool.on('error', (err) => {
    console.error('Unexpected PostgreSQL pool error:', err);
});
