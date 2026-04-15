import cors from 'cors';
import 'dotenv/config';
import express from 'express';
import { errorMiddleware, notFoundMiddleware } from './middleware/error.middleware';
import authRouter from './modules/auth/auth.routes';
import moveRouter from './modules/move/move.routes';
import tasksRouter from './modules/tasks/tasks.routes';
import profileRoutes from './modules/users/profil.routes';

const app = express();

const allowedOrigins = [
  'http://localhost:8081',
  'http://127.0.0.1:8081',
  'https://api.halloch.ch',
];

app.use(
  cors({
    origin: allowedOrigins,
    methods: ['GET', 'POST', 'PATCH', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization'],
  })
);

app.options('*', cors());

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