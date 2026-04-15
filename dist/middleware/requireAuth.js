"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.requireAuth = requireAuth;
const jsonwebtoken_1 = __importDefault(require("jsonwebtoken"));
const error_middleware_1 = require("./error.middleware");
function requireAuth(req, _res, next) {
    try {
        const authHeader = req.headers.authorization;
        if (!authHeader || !authHeader.startsWith('Bearer ')) {
            return next(new error_middleware_1.AppError('Authorization token fehlt.', 401));
        }
        const token = authHeader.substring(7);
        const secret = process.env.JWT_SECRET;
        if (!secret) {
            return next(new error_middleware_1.AppError('JWT_SECRET fehlt in der Server-Konfiguration.', 500, {
                expose: false,
                code: 'JWT_SECRET_MISSING',
            }));
        }
        const decoded = jsonwebtoken_1.default.verify(token, secret);
        if (typeof decoded === 'string') {
            return next(new error_middleware_1.AppError('Ungueltiger Token.', 401));
        }
        const userId = decoded.userId || decoded.id || decoded.sub;
        if (!userId || typeof userId !== 'string') {
            return next(new error_middleware_1.AppError('Token enthaelt keine gueltige User-ID.', 401));
        }
        req.user = {
            id: userId,
            email: typeof decoded.email === 'string' ? decoded.email : undefined,
        };
        return next();
    }
    catch (error) {
        console.error('Auth error:', error);
        return next(new error_middleware_1.AppError('Ungueltiger oder abgelaufener Token.', 401));
    }
}
