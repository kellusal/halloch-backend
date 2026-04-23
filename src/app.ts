import cors, { type CorsOptionsDelegate } from 'cors';
import 'dotenv/config';
import express from 'express';
import path from 'path';
import { env } from './config/env';
import { pool } from './db/pool';
import { errorMiddleware, notFoundMiddleware } from './middleware/error.middleware';
import authRouter from './modules/auth/auth.routes';
import moveRouter from './modules/move/move.routes';
import tasksRouter from './modules/tasks/tasks.routes';
import profileRoutes from './modules/users/profil.routes';

const app = express();

app.use('/generated', express.static(path.join(process.cwd(), 'public/generated')));

const allowedOrigins = env.FRONTEND_ORIGINS;

const corsOptionsDelegate: CorsOptionsDelegate = (req, callback) => {
  const requestOrigin =
    typeof req.headers.origin === 'string' ? req.headers.origin : undefined;
  const isAllowedOrigin = !requestOrigin || allowedOrigins.includes(requestOrigin);

  callback(null, {
    origin: isAllowedOrigin,
    methods: ['GET', 'POST', 'PATCH', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization'],
    optionsSuccessStatus: 204,
  });
};

app.use(cors(corsOptionsDelegate));
app.options(/.*/, cors(corsOptionsDelegate));

app.use(express.json({ limit: '1mb' }));

app.get('/health', async (_req, res) => {
  try {
    await pool.query('SELECT 1');

    res.status(200).json({
      ok: true,
      db: 'ok',
    });
  } catch (error) {
    console.error('[HEALTHCHECK_ERROR]', {
      error: error instanceof Error ? error.message : String(error),
      stack: error instanceof Error ? error.stack ?? null : null,
    });

    res.status(503).json({
      ok: false,
      db: 'error',
    });
  }
});

app.use('/auth', authRouter);
app.use('/move', moveRouter);
app.use('/tasks', tasksRouter);
app.use('/profile', profileRoutes);

app.use(notFoundMiddleware);
app.use(errorMiddleware);

export default app;