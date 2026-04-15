import { NextFunction, Request, Response } from 'express';
import jwt from 'jsonwebtoken';

type JwtPayload = {
  sub: string;
  email?: string;
};

export function requireAuth(req: Request, res: Response, next: NextFunction) {
  try {
    const authHeader = req.headers.authorization;

    if (!authHeader) {
      return res.status(401).json({ message: 'Missing authorization header' });
    }

    if (!authHeader.startsWith('Bearer ')) {
      return res.status(401).json({ message: 'Invalid authorization header' });
    }

    const token = authHeader.replace('Bearer ', '').trim();

    if (!token) {
      return res.status(401).json({ message: 'Missing token' });
    }

    const secret = process.env.JWT_SECRET;

    if (!secret) {
      return res.status(500).json({ message: 'JWT_SECRET is not configured' });
    }

    const decoded = jwt.verify(token, secret) as JwtPayload;
    const userId = Number(decoded.sub);

    if (!userId || Number.isNaN(userId)) {
      return res.status(401).json({ message: 'Invalid token payload' });
    }

    req.user = {
      id: userId,
      email: decoded.email,
    };

    return next();
  } catch {
    return res.status(401).json({ message: 'Invalid or expired token' });
  }
}