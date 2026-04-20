import { NextFunction, Request, Response } from 'express';
import { AppError } from '../../middleware/error.middleware';
import * as authService from './auth.service';

function logAuthError(event: string, req: Request, error: unknown) {
  console.error(event, {
    route: `${req.method} ${req.originalUrl}`,
    userId: req.user?.id ?? null,
    error: error instanceof Error ? error.message : String(error),
    stack: error instanceof Error ? error.stack ?? null : null,
  });
}

function mapAuthError(error: unknown) {
  const message =
    error instanceof Error ? error.message : 'Internal server error';

  if (
    message === 'Email already exists' ||
    message === 'Email is required' ||
    message === 'Password is required' ||
    message === 'Password must be at least 6 characters long'
  ) {
    return new AppError(message, 400);
  }

  if (
    message === 'Invalid email or password' ||
    message === 'User is inactive'
  ) {
    return new AppError(message, 401);
  }

  if (message === 'User not found') {
    return new AppError(message, 404);
  }

  return error instanceof Error
    ? error
    : new AppError('Internal server error', 500, { expose: false });
}

export async function register(
  req: Request,
  res: Response,
  next: NextFunction
) {
  try {
    const result = await authService.register({
      email: req.body.email,
      password: req.body.password,
      firstName: req.body.firstName,
      lastName: req.body.lastName,
      language: req.body.language,
    });

    return res.status(201).json(result);
  } catch (error) {
    return next(mapAuthError(error));
  }
}

export async function login(
  req: Request,
  res: Response,
  next: NextFunction
) {
  try {
    const result = await authService.login({
      email: req.body.email,
      password: req.body.password,
    });

    console.info('[AUTH_LOGIN_SUCCESS]', {
      route: `${req.method} ${req.originalUrl}`,
      userId: result.user.id,
    });

    return res.status(200).json(result);
  } catch (error) {
    logAuthError('[AUTH_LOGIN_ERROR]', req, error);
    return next(mapAuthError(error));
  }
}

export async function me(req: Request, res: Response, next: NextFunction) {
  try {
    if (!req.user?.id) {
      return next(new AppError('Unauthorized', 401));
    }

    const result = await authService.getMe(Number(req.user.id));
    return res.status(200).json(result);
  } catch (error) {
    logAuthError('[AUTH_ME_ERROR]', req, error);
    return next(mapAuthError(error));
  }
}
