"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const express_1 = require("express");
const requireAuth_1 = require("../../middleware/requireAuth");
const profil_controller_1 = require("./profil.controller");
const router = (0, express_1.Router)();
router.get('/me', requireAuth_1.requireAuth, profil_controller_1.getMyProfileHandler);
router.put('/me', requireAuth_1.requireAuth, profil_controller_1.updateMyProfileHandler);
exports.default = router;
