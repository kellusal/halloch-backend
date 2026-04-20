import 'dotenv/config';

type NodeEnv = 'development' | 'test' | 'production';

function readString(name: string, fallback?: string): string {
  const value = process.env[name]?.trim();
  if (value) return value;
  if (fallback !== undefined) return fallback;
  throw new Error(`Missing required environment variable: ${name}`);
}

function readNumber(name: string, fallback: number): number {
  const raw = process.env[name]?.trim();
  if (!raw) return fallback;
  const parsed = Number(raw);
  if (Number.isNaN(parsed) || parsed <= 0) {
    throw new Error(`Environment variable ${name} must be a positive number`);
  }
  return parsed;
}

function parseOrigins(value: string): string[] {
  return value
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean);
}

const REQUIRED_PROD_ORIGINS = ['https://app.halloch.ch', 'https://halloch.ch'];

const DEV_ORIGINS = [
  'http://localhost:8081',
  'http://127.0.0.1:8081',
  'http://localhost:19006',
  'http://127.0.0.1:19006',
  'http://localhost:3000',
  'http://127.0.0.1:3000',
];

const NODE_ENV = (process.env.NODE_ENV?.trim() || 'development') as NodeEnv;
const PORT = readNumber('PORT', 3001);
const DATABASE_URL = readString('DATABASE_URL');
const JWT_SECRET = readString('JWT_SECRET');
const JWT_EXPIRES_IN = readString('JWT_EXPIRES_IN', '7d');
const configuredOrigins = parseOrigins(
  readString('FRONTEND_ORIGINS', 'http://localhost:8081,http://127.0.0.1:8081')
);
const mergedConfiguredOrigins = Array.from(
  new Set([...configuredOrigins, ...REQUIRED_PROD_ORIGINS])
);
const FRONTEND_ORIGINS =
  NODE_ENV === 'production'
    ? mergedConfiguredOrigins
    : Array.from(new Set([...mergedConfiguredOrigins, ...DEV_ORIGINS]));
const RATE_LIMIT_WINDOW_MS = readNumber('RATE_LIMIT_WINDOW_MS', 15 * 60 * 1000);
const RATE_LIMIT_MAX = readNumber('RATE_LIMIT_MAX', 300);
const AUTH_RATE_LIMIT_MAX = readNumber('AUTH_RATE_LIMIT_MAX', 25);

if (NODE_ENV === 'production' && JWT_SECRET.length < 32) {
  throw new Error('JWT_SECRET must have at least 32 characters in production');
}

export const env = {
  NODE_ENV,
  PORT,
  DATABASE_URL,
  JWT_SECRET,
  JWT_EXPIRES_IN,
  FRONTEND_ORIGINS,
  RATE_LIMIT_WINDOW_MS,
  RATE_LIMIT_MAX,
  AUTH_RATE_LIMIT_MAX,
  IS_PRODUCTION: NODE_ENV === 'production',
};
