import 'dotenv/config';
import cors, { type CorsOptionsDelegate } from 'cors';
import express from 'express';
import { env } from './config/env';
import { errorMiddleware, notFoundMiddleware } from './middleware/error.middleware';
import authRouter from './modules/auth/auth.routes';
import moveRouter from './modules/move/move.routes';
import tasksRouter from './modules/tasks/tasks.routes';
import profileRoutes from './modules/users/profil.routes';

const app = express();

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

app.use(express.json());

app.get('/health', (_req, res) => {
  res.status(200).json({ ok: true });
});

app.use('/auth', authRouter);
app.use('/move', moveRouter);
app.use('/tasks', tasksRouter);
app.use('/profile', profileRoutes);

app.use(notFoundMiddleware);
app.use(errorMiddleware);

export default app;