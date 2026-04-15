"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.register = register;
exports.login = login;
exports.me = me;
const error_middleware_1 = require("../../middleware/error.middleware");
const authService = __importStar(require("./auth.service"));
function mapAuthError(error) {
    const message = error instanceof Error ? error.message : 'Internal server error';
    if (message === 'Email already exists' ||
        message === 'Email is required' ||
        message === 'Password is required' ||
        message === 'Password must be at least 6 characters long') {
        return new error_middleware_1.AppError(message, 400);
    }
    if (message === 'Invalid email or password' ||
        message === 'User is inactive') {
        return new error_middleware_1.AppError(message, 401);
    }
    if (message === 'User not found') {
        return new error_middleware_1.AppError(message, 404);
    }
    return error instanceof Error
        ? error
        : new error_middleware_1.AppError('Internal server error', 500, { expose: false });
}
async function register(req, res, next) {
    try {
        const result = await authService.register({
            email: req.body.email,
            password: req.body.password,
            firstName: req.body.firstName,
            lastName: req.body.lastName,
            language: req.body.language,
        });
        return res.status(201).json(result);
    }
    catch (error) {
        return next(mapAuthError(error));
    }
}
async function login(req, res, next) {
    try {
        const result = await authService.login({
            email: req.body.email,
            password: req.body.password,
        });
        return res.status(200).json(result);
    }
    catch (error) {
        return next(mapAuthError(error));
    }
}
async function me(req, res, next) {
    try {
        if (!req.user?.id) {
            return next(new error_middleware_1.AppError('Unauthorized', 401));
        }
        const result = await authService.getMe(Number(req.user.id));
        return res.status(200).json(result);
    }
    catch (error) {
        return next(mapAuthError(error));
    }
}
