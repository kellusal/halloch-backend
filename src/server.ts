import 'dotenv/config';
import app from './app';
import { pool } from './db/pool';
import profileRoutes from './modules/users/profil.routes';

app.use('/profile', profileRoutes);

const port = Number(process.env.PORT) || 3001;

async function startServer() {
  try {
    await pool.query('SELECT 1');
    console.log('Database connection successful');

    app.listen(port, '0.0.0.0', () => {
      console.log(`Server is running on http://0.0.0.0:${port}`);
    });
  } catch (error) {
    console.error('Failed to start server:', error);
    process.exit(1);
  }
}

startServer();