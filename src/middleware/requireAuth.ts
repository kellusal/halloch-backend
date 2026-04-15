import { NextFunction, Request, Response } from 'express';
import jwt, { JwtPayload } from 'jsonwebtoken';
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
    const secret = process.env.JWT_SECRET;

    if (!secret) {
      return next(
        new AppError('JWT_SECRET fehlt in der Server-Konfiguration.', 500, {
          expose: false,
          code: 'JWT_SECRET_MISSING',
        })
      );
    }

    const decoded = jwt.verify(token, secret) as TokenPayload | string;

    if (typeof decoded === 'string') {
      return next(new AppError('Ungueltiger Token.', 401));
    }

    const userId = decoded.userId || decoded.id || decoded.sub;

    if (!userId || typeof userId !== 'string') {
      return next(new AppError('Token enthaelt keine gueltige User-ID.', 401));
    }

    req.user = {
      id: userId,
      email: typeof decoded.email === 'string' ? decoded.email : undefined,
    };

    return next();
  } catch (error) {
    console.error('Auth error:', error);
    return next(new AppError('Ungueltiger oder abgelaufener Token.', 401));
  }
}