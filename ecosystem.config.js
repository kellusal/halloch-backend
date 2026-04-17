module.exports = {
  apps: [
    {
      name: 'halloch-backend',
      script: 'dist/server.js',
      cwd: '/home/ubuntu/halloch-backend',
      instances: 1,
      exec_mode: 'fork',
      autorestart: true,
      watch: false,
      max_memory_restart: '300M',
      env: {
        NODE_ENV: 'production',
        PORT: 3001
      }
    }
  ]
};
