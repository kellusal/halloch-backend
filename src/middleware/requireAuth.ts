import { NextFunction, Request, Response } from 'express';
import jwt, { JwtPayload } from 'jsonwebtoken';

type TokenPayload = JwtPayload & {
  userId?: string;
  id?: string;
  email?: string;
  sub?: string;
};

export function requireAuth(req: Request, res: Response, next: NextFunction) {
  try {
    const authHeader = req.headers.authorization;

    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return res.status(401).json({
        message: 'Authorization token fehlt.',
      });
    }

    const token = authHeader.substring(7);
    const secret = process.env.JWT_SECRET;

    if (!secret) {
      return res.status(500).json({
        message: 'JWT_SECRET fehlt in der Server-Konfiguration.',
      });
    }

    const decoded = jwt.verify(token, secret) as TokenPayload | string;

    if (typeof decoded === 'string') {
      return res.status(401).json({
        message: 'Ungültiger Token.',
      });
    }

    const userId = decoded.userId || decoded.id || decoded.sub;

    if (!userId || typeof userId !== 'string') {
      return res.status(401).json({
        message: 'Token enthält keine gültige User-ID.',
      });
    }

    req.user = {
      id: userId,
      email: typeof decoded.email === 'string' ? decoded.email : undefined,
    };

    return next();
  } catch (error) {
    console.error('Auth error:', error);

    return res.status(401).json({
      message: 'Ungültiger oder abgelaufener Token.',
    });
  }
}