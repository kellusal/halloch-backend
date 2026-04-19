import app from './app';
import { env } from './config/env';
import { pool } from './db/pool';

const port = env.PORT;
let shuttingDown = false;

function maskDbConnection(url: string) {
  try {
    const parsed = new URL(url);
    const dbName = parsed.pathname.replace('/', '');
    return {
      host: parsed.hostname,
      port: parsed.port || '5432',
      database: dbName || 'unknown',
      user: parsed.username || 'unknown',
    };
  } catch {
    return {
      host: 'unknown',
      port: 'unknown',
      database: 'unknown',
      user: 'unknown',
    };
  }
}

async function startServer() {
  try {
    const db = maskDbConnection(env.DATABASE_URL);
    console.log(
      `[BOOT] env=${env.NODE_ENV} port=${port} db=${db.user}@${db.host}:${db.port}/${db.database}`
    );
    console.log(`[BOOT] allowedOrigins=${env.FRONTEND_ORIGINS.join(',')}`);

    await pool.query('SELECT 1');
    console.log('Database connection successful');

    const server = app.listen(port, '0.0.0.0', () => {
      console.log(`Server is running on http://0.0.0.0:${port}`);
    });

    const shutdown = async (signal: NodeJS.Signals) => {
      if (shuttingDown) return;
      shuttingDown = true;

      console.log(`Received ${signal}, shutting down gracefully...`);

      server.close(async () => {
        try {
          await pool.end();
          console.log('HTTP server and DB pool closed');
          process.exit(0);
        } catch (error) {
          console.error('Error while closing DB pool:', error);
          process.exit(1);
        }
      });

      setTimeout(() => {
        console.error('Forced shutdown after timeout');
        process.exit(1);
      }, 10000).unref();
    };

    process.on('SIGINT', () => {
      void shutdown('SIGINT');
    });

    process.on('SIGTERM', () => {
      void shutdown('SIGTERM');
    });

    process.on('unhandledRejection', (reason) => {
      console.error('Unhandled promise rejection:', reason);
    });

    process.on('uncaughtException', (error) => {
      console.error('Uncaught exception:', error);
      void shutdown('SIGTERM');
    });
  } catch (error) {
    console.error('Failed to start server:', error);
    process.exit(1);
  }
}

startServer();
