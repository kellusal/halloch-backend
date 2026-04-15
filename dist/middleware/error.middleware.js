"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.AppError = void 0;
exports.notFoundMiddleware = notFoundMiddleware;
exports.errorMiddleware = errorMiddleware;
exports.sendInternalServerError = sendInternalServerError;
class AppError extends Error {
    constructor(message, statusCode = 500, options) {
        super(message);
        this.name = 'AppError';
        this.statusCode = statusCode;
        this.code = options?.code;
        this.expose = options?.expose ?? statusCode < 500;
    }
}
exports.AppError = AppError;
function notFoundMiddleware(req, res, _next) {
    return res.status(404).json({
        message: `Route not found: ${req.method} ${req.originalUrl}`,
    });
}
function errorMiddleware(error, _req, res, _next) {
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
function sendInternalServerError(res, error, context) {
    if (context) {
        console.error(context, error);
    }
    else {
        console.error('Unhandled backend error:', error);
    }
    return res.status(500).json({
        message: 'Internal server error',
    });
}
