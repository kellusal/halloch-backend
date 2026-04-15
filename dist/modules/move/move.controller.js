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
exports.createMoveCase = createMoveCase;
exports.getMoveCaseById = getMoveCaseById;
exports.getMoveCaseTasks = getMoveCaseTasks;
const error_middleware_1 = require("../../middleware/error.middleware");
const moveService = __importStar(require("./move.services"));
function mapMoveServiceError(error) {
    const message = error instanceof Error ? error.message : 'Internal server error';
    if (message === 'User is required' ||
        message === 'To city is required' ||
        message === 'Move date is required' ||
        message === 'Destination city not found' ||
        message === 'Origin city not found') {
        return new error_middleware_1.AppError(message, 400);
    }
    if (message === 'Move case not found') {
        return new error_middleware_1.AppError(message, 404);
    }
    return error instanceof Error
        ? error
        : new error_middleware_1.AppError('Internal server error', 500, { expose: false });
}
async function createMoveCase(req, res, next) {
    try {
        if (!req.user?.id) {
            return next(new error_middleware_1.AppError('Unauthorized', 401));
        }
        const result = await moveService.createMoveCase({
            userId: Number(req.user.id),
            fromCity: req.body.fromCity,
            toCity: req.body.toCity,
            moveDate: req.body.moveDate,
            hasCar: Boolean(req.body.hasCar),
            hasChildren: Boolean(req.body.hasChildren),
        });
        return res.status(201).json(result);
    }
    catch (error) {
        return next(mapMoveServiceError(error));
    }
}
async function getMoveCaseById(req, res, next) {
    try {
        if (!req.user?.id) {
            return next(new error_middleware_1.AppError('Unauthorized', 401));
        }
        const { caseId } = req.params;
        const result = await moveService.getMoveCaseById(caseId, Number(req.user.id));
        return res.status(200).json(result);
    }
    catch (error) {
        return next(mapMoveServiceError(error));
    }
}
async function getMoveCaseTasks(req, res, next) {
    try {
        if (!req.user?.id) {
            return next(new error_middleware_1.AppError('Unauthorized', 401));
        }
        const { caseId } = req.params;
        const result = await moveService.getMoveCaseTasks(caseId, Number(req.user.id));
        return res.status(200).json(result);
    }
    catch (error) {
        return next(mapMoveServiceError(error));
    }
}
