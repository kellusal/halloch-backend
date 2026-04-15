import { NextFunction, Request, Response } from 'express';

export class AppError extends Error {
  statusCode: number;
  code?: string;
  expose: boolean;

  constructor(
    message: string,
    statusCode = 500,
    options?: { code?: string; expose?: boolean }
  ) {
    super(message);
    this.name = 'AppError';
    this.statusCode = statusCode;
    this.code = options?.code;
    this.expose = options?.expose ?? statusCode < 500;
  }
}

export function notFoundMiddleware(
  req: Request,
  res: Response,
  _next: NextFunction
) {
  return res.status(404).json({
    message: `Route not found: ${req.method} ${req.originalUrl}`,
  });
}

export function errorMiddleware(
  error: unknown,
  _req: Request,
  res: Response,
  _next: NextFunction
) {
  if (error instanceof AppError) {
    return res.status(error.statusCode).json({
      message: error.expose ? error.message : 'Internal server error',
      ...(error.code ? { code: error.code } : {}),
    });
  }

  console.error('Unhandled backend error:', error);

  return res.status(500).json({
    message: 'Internal server error',
  });
}

export function sendInternalServerError(
  res: Response,
  error: unknown,
  context?: string
) {
  if (context) {
    console.error(context, error);
  } else {
    console.error('Unhandled backend error:', error);
  }

  return res.status(500).json({
    message: 'Internal server error',
  });
}
