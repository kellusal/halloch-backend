"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.register = register;
exports.login = login;
exports.getMe = getMe;
const bcrypt_1 = __importDefault(require("bcrypt"));
const jsonwebtoken_1 = __importDefault(require("jsonwebtoken"));
const pool_1 = require("../../db/pool");
function signToken(user) {
    const secret = process.env.JWT_SECRET;
    if (!secret) {
        throw new Error('JWT_SECRET is not configured');
    }
    const expiresIn = (process.env.JWT_EXPIRES_IN || '7d');
    return jsonwebtoken_1.default.sign({
        sub: String(user.id),
        email: user.email,
    }, secret, {
        expiresIn,
    });
}
function mapUser(row) {
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
async function register(input) {
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
    const existingUserResult = await pool_1.pool.query(`
      SELECT id
      FROM app.users
      WHERE email = $1
      LIMIT 1
    `, [email]);
    if (existingUserResult.rowCount && existingUserResult.rowCount > 0) {
        throw new Error('Email already exists');
    }
    const passwordHash = await bcrypt_1.default.hash(password, 10);
    const insertResult = await pool_1.pool.query(`
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
    `, [email, passwordHash, firstName, lastName, language]);
    const userRow = insertResult.rows[0];
    const token = signToken({ id: userRow.id, email: userRow.email });
    return {
        token,
        user: mapUser(userRow),
    };
}
async function login(input) {
    const email = input.email.trim().toLowerCase();
    const password = input.password;
    if (!email) {
        throw new Error('Email is required');
    }
    if (!password) {
        throw new Error('Password is required');
    }
    const result = await pool_1.pool.query(`
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
    `, [email]);
    if (!result.rowCount || result.rowCount === 0) {
        throw new Error('Invalid email or password');
    }
    const userRow = result.rows[0];
    if (!userRow.is_active) {
        throw new Error('User is inactive');
    }
    const isValidPassword = await bcrypt_1.default.compare(password, userRow.password_hash);
    if (!isValidPassword) {
        throw new Error('Invalid email or password');
    }
    const token = signToken({ id: userRow.id, email: userRow.email });
    return {
        token,
        user: mapUser(userRow),
    };
}
async function getMe(userId) {
    const result = await pool_1.pool.query(`
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
    `, [userId]);
    if (!result.rowCount || result.rowCount === 0) {
        throw new Error('User not found');
    }
    return {
        user: mapUser(result.rows[0]),
    };
}
