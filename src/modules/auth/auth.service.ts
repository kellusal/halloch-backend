import bcrypt from 'bcrypt';
import jwt from 'jsonwebtoken';
import { pool } from '../../db/pool';

type RegisterInput = {
  email: string;
  password: string;
  firstName?: string;
  lastName?: string;
  language?: 'de' | 'fr' | 'en';
};

type LoginInput = {
  email: string;
  password: string;
};

type DbUserRow = {
  id: number;
  email: string;
  password_hash: string;
  first_name: string | null;
  last_name: string | null;
  language: 'de' | 'fr' | 'en' | null;
  is_active: boolean;
  created_at: Date;
  updated_at: Date;
};

function signToken(user: { id: number; email: string }) {
  const secret = process.env.JWT_SECRET;

  if (!secret) {
    throw new Error('JWT_SECRET is not configured');
  }

  return jwt.sign(
    {
      sub: String(user.id),
      email: user.email,
    },
    secret,
    {
      expiresIn: process.env.JWT_EXPIRES_IN || '7d',
    }
  );
}

function mapUser(row: Omit<DbUserRow, 'password_hash'> | DbUserRow) {
  return {
    id: row.id,
    email: row.email,
    firstName: row.first_name,
    lastName: row.last_name,
    language: row.language ?? 'de',
    isActive: row.is_active,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

export async function register(input: RegisterInput) {
  const email = input.email.trim().toLowerCase();
  const password = input.password;
  const firstName = input.firstName?.trim() || null;
  const lastName = input.lastName?.trim() || null;
  const language = input.language ?? 'de';

  if (!email) {
    throw new Error('Email is required');
  }

  if (!password) {
    throw new Error('Password is required');
  }

  if (password.length < 6) {
    throw new Error('Password must be at least 6 characters long');
  }

  const existingUserResult = await pool.query<{ id: number }>(
    `
      SELECT id
      FROM app.users
      WHERE email = $1
      LIMIT 1
    `,
    [email]
  );

  if (existingUserResult.rowCount && existingUserResult.rowCount > 0) {
    throw new Error('Email already exists');
  }

  const passwordHash = await bcrypt.hash(password, 10);

  const insertResult = await pool.query<DbUserRow>(
    `
      INSERT INTO app.users (
        email,
        password_hash,
        first_name,
        last_name,
        language
      )
      VALUES ($1, $2, $3, $4, $5)
      RETURNING
        id,
        email,
        password_hash,
        first_name,
        last_name,
        language,
        is_active,
        created_at,
        updated_at
    `,
    [email, passwordHash, firstName, lastName, language]
  );

  const userRow = insertResult.rows[0];
  const token = signToken({ id: userRow.id, email: userRow.email });

  return {
    token,
    user: mapUser(userRow),
  };
}

export async function login(input: LoginInput) {
  const email = input.email.trim().toLowerCase();
  const password = input.password;

  if (!email) {
    throw new Error('Email is required');
  }

  if (!password) {
    throw new Error('Password is required');
  }

  const result = await pool.query<DbUserRow>(
    `
      SELECT
        id,
        email,
        password_hash,
        first_name,
        last_name,
        language,
        is_active,
        created_at,
        updated_at
      FROM app.users
      WHERE email = $1
      LIMIT 1
    `,
    [email]
  );

  if (!result.rowCount || result.rowCount === 0) {
    throw new Error('Invalid email or password');
  }

  const userRow = result.rows[0];

  if (!userRow.is_active) {
    throw new Error('User is inactive');
  }

  const isValidPassword = await bcrypt.compare(password, userRow.password_hash);

  if (!isValidPassword) {
    throw new Error('Invalid email or password');
  }

  const token = signToken({ id: userRow.id, email: userRow.email });

  return {
    token,
    user: mapUser(userRow),
  };
}

export async function getMe(userId: number) {
  const result = await pool.query<Omit<DbUserRow, 'password_hash'>>(
    `
      SELECT
        id,
        email,
        first_name,
        last_name,
        language,
        is_active,
        created_at,
        updated_at
      FROM app.users
      WHERE id = $1
      LIMIT 1
    `,
    [userId]
  );

  if (!result.rowCount || result.rowCount === 0) {
    throw new Error('User not found');
  }

  return {
    user: mapUser(result.rows[0]),
  };
}