import cors from 'cors';
import 'dotenv/config';
import express from 'express';
import authRouter from './modules/auth/auth.routes';
import moveRouter from './modules/move/move.routes';
import tasksRouter from './modules/tasks/tasks.routes';

const app = express();

app.use(
  cors({
    origin: '*',
    methods: ['GET', 'POST', 'PATCH', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization'],
  })
);

app.use(express.json());

app.get('/health', (_req, res) => {
  res.status(200).json({ ok: true });
});

app.use('/auth', authRouter);
app.use('/move', moveRouter);
app.use('/tasks', tasksRouter);

export default app;