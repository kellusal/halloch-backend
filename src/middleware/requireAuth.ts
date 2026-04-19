import { NextFunction, Request, Response } from 'express';
import jwt, { JwtPayload } from 'jsonwebtoken';
import { env } from '../config/env';
import { AppError } from './error.middleware';

type TokenPayload = JwtPayload & {
  userId?: string;
  id?: string;
  email?: string;
  sub?: string;
};

export function requireAuth(req: Request, _res: Response, next: NextFunction) {
  try {
    const authHeader = req.headers.authorization;

    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return next(new AppError('Authorization token fehlt.', 401));
    }

    const token = authHeader.substring(7);
    const secret = env.JWT_SECRET;

    const decoded = jwt.verify(token, secret) as TokenPayload | string;

    if (typeof decoded === 'string') {
      return next(new AppError('Ungültiger Token.', 401));
    }

    const userId = decoded.userId || decoded.id || decoded.sub;

    if (!userId || typeof userId !== 'string') {
      return next(new AppError('Token enthält keine gültige User-ID.', 401));
    }

    req.user = {
      id: userId,
      email: typeof decoded.email === 'string' ? decoded.email : undefined,
    };

    return next();
  } catch (error) {
    console.error('Auth error:', error);
    return next(new AppError('Ungültiger oder abgelaufener Token.', 401));
  }
}
