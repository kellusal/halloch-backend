"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const cors_1 = __importDefault(require("cors"));
require("dotenv/config");
const express_1 = __importDefault(require("express"));
const error_middleware_1 = require("./middleware/error.middleware");
const auth_routes_1 = __importDefault(require("./modules/auth/auth.routes"));
const move_routes_1 = __importDefault(require("./modules/move/move.routes"));
const tasks_routes_1 = __importDefault(require("./modules/tasks/tasks.routes"));
const profil_routes_1 = __importDefault(require("./modules/users/profil.routes"));
const app = (0, express_1.default)();
app.use((0, cors_1.default)({
    origin: '*',
    methods: ['GET', 'POST', 'PATCH', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization'],
}));
app.use(express_1.default.json());
app.get('/health', (_req, res) => {
    res.status(200).json({ ok: true });
});
app.use('/auth', auth_routes_1.default);
app.use('/move', move_routes_1.default);
app.use('/tasks', tasks_routes_1.default);
app.use('/profile', profil_routes_1.default);
app.use(error_middleware_1.notFoundMiddleware);
app.use(error_middleware_1.errorMiddleware);
exports.default = app;
