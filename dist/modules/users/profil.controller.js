"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getMyProfileHandler = getMyProfileHandler;
exports.updateMyProfileHandler = updateMyProfileHandler;
const error_middleware_1 = require("../../middleware/error.middleware");
const move_task_sync_1 = require("../move/move.task-sync");
const profil_repository_1 = require("./profil.repository");
async function getMyProfileHandler(req, res) {
    try {
        const userId = req.user?.id;
        if (!userId) {
            return res.status(401).json({
                message: 'Unauthorized',
            });
        }
        const profile = await (0, profil_repository_1.getMyProfile)(String(userId));
        return res.status(200).json(profile);
    }
    catch (error) {
        return (0, error_middleware_1.sendInternalServerError)(res, error, 'Error loading my profile:');
    }
}
async function updateMyProfileHandler(req, res) {
    try {
        const userId = req.user?.id;
        if (!userId) {
            return res.status(401).json({
                message: 'Unauthorized',
            });
        }
        const { lastName, dateOfBirth, nationality, maritalStatus, phone, street, zip, city, canton, hasCar, hasChildren, hasDog, childrenCount, healthInsuranceName, employerName, } = req.body ?? {};
        const updatedProfile = await (0, profil_repository_1.updateMyProfile)(String(userId), {
            lastName,
            dateOfBirth,
            nationality,
            maritalStatus,
            phone,
            street,
            zip,
            city,
            canton,
            hasCar,
            hasChildren,
            hasDog,
            childrenCount,
            healthInsuranceName,
            employerName,
        });
        // Nach erfolgreicher Profiländerung alle aktiven Umzugsfälle neu synchronisieren.
        await (0, move_task_sync_1.refreshActiveMoveCasesForUser)(Number(userId));
        return res.status(200).json(updatedProfile);
    }
    catch (error) {
        return (0, error_middleware_1.sendInternalServerError)(res, error, 'Error updating my profile:');
    }
}
