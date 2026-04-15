"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const express_1 = require("express");
const pool_1 = require("../../db/pool");
const error_middleware_1 = require("../../middleware/error.middleware");
const requireAuth_1 = require("../../middleware/requireAuth");
const router = (0, express_1.Router)();
router.get('/overview', requireAuth_1.requireAuth, async (req, res) => {
    try {
        const userId = req.user?.id;
        if (!userId) {
            return res.status(401).json({
                message: 'Benutzer konnte nicht aus dem Token gelesen werden.',
            });
        }
        const latestMoveCaseResult = await pool_1.pool.query(`
      SELECT
        mc.id,
        mc.move_date,
        mc.status,
        mc.has_car,
        mc.has_children,
        fc.name AS from_city_name,
        tc.name AS to_city_name
      FROM move_cases mc
      LEFT JOIN move_cities fc ON fc.id = mc.from_city_id
      LEFT JOIN move_cities tc ON tc.id = mc.to_city_id
      WHERE mc.user_id = $1
      ORDER BY mc.created_at DESC
      LIMIT 1
      `, [userId]);
        const latestMoveCase = latestMoveCaseResult.rows[0] ?? null;
        let modules = [];
        let tasks = [];
        let doneTasks = [];
        let summary = {
            open: 0,
            today: 0,
            modules: 0,
        };
        let focusTask = null;
        if (latestMoveCase) {
            const moduleTasksResult = await pool_1.pool.query(`
        SELECT
          id,
          case_id,
          category,
          title,
          description,
          status,
          due_date,
          sort_order,
          external_url,
          link_label,
          completed_at,
          created_at
        FROM move_case_tasks
        WHERE case_id = $1
        ORDER BY
          CASE WHEN status = 'open' THEN 0 ELSE 1 END,
          sort_order ASC,
          due_date ASC NULLS LAST,
          created_at ASC
        `, [latestMoveCase.id]);
            const moduleTasks = moduleTasksResult.rows;
            const openTasks = moduleTasks.filter((task) => task.status === 'open');
            const doneModuleTasks = moduleTasks.filter((task) => task.status === 'done');
            const progress = moduleTasks.length === 0
                ? 0
                : Math.round((doneModuleTasks.length / moduleTasks.length) * 100);
            const nextTask = openTasks[0] ?? null;
            modules = [
                {
                    id: 'umzug',
                    title: 'Umzug',
                    subtitle: `${latestMoveCase.from_city_name ?? '-'} -> ${latestMoveCase.to_city_name ?? '-'}`,
                    progress,
                    openCount: openTasks.length,
                    nextTask: nextTask?.title ?? null,
                    route: `/umzug/process?caseId=${latestMoveCase.id}`,
                    tone: 'red',
                    statusLabel: 'Aktiv',
                },
            ];
            tasks = openTasks.slice(0, 6).map((task) => ({
                id: task.id,
                title: task.title,
                module: 'Umzug',
                due: formatDueLabel(task.due_date),
                status: isDueSoon(task.due_date) ? 'soon' : 'open',
                priority: getPriority(task.due_date),
                route: `/umzug/task?caseId=${latestMoveCase.id}&taskId=${task.id}`,
            }));
            doneTasks = doneModuleTasks
                .sort((a, b) => {
                const aDate = a.completed_at ? new Date(a.completed_at).getTime() : 0;
                const bDate = b.completed_at ? new Date(b.completed_at).getTime() : 0;
                return bDate - aDate;
            })
                .slice(0, 4)
                .map((task) => ({
                id: task.id,
                title: task.title,
                module: 'Umzug',
                when: formatDoneLabel(task.completed_at),
            }));
            focusTask = openTasks[0]
                ? {
                    title: openTasks[0].title,
                    module: 'Umzug',
                    due: formatDueLabel(openTasks[0].due_date),
                    route: `/umzug/task?caseId=${latestMoveCase.id}&taskId=${openTasks[0].id}`,
                }
                : null;
            summary = {
                open: openTasks.length,
                today: openTasks.filter((task) => isDueToday(task.due_date)).length,
                modules: modules.length,
            };
        }
        return res.status(200).json({
            summary,
            focusTask,
            modules,
            tasks,
            doneTasks,
        });
    }
    catch (error) {
        return (0, error_middleware_1.sendInternalServerError)(res, error, 'Error loading tasks overview:');
    }
});
function isDueToday(value) {
    if (!value)
        return false;
    const due = new Date(value);
    const now = new Date();
    return (due.getFullYear() === now.getFullYear() &&
        due.getMonth() === now.getMonth() &&
        due.getDate() === now.getDate());
}
function isDueSoon(value) {
    if (!value)
        return false;
    const due = new Date(value);
    const now = new Date();
    const diffMs = due.getTime() - now.getTime();
    const diffDays = diffMs / (1000 * 60 * 60 * 24);
    return diffDays >= 0 && diffDays <= 3;
}
function getPriority(value) {
    if (!value)
        return 'low';
    if (isDueToday(value))
        return 'high';
    if (isDueSoon(value))
        return 'medium';
    return 'low';
}
function formatDueLabel(value) {
    if (!value)
        return 'Spaeter';
    const due = new Date(value);
    const now = new Date();
    const diffMs = due.getTime() - now.getTime();
    const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));
    if (isDueToday(value))
        return 'Heute';
    if (diffDays === 1)
        return 'Morgen';
    if (diffDays > 1 && diffDays <= 6)
        return `In ${diffDays} Tagen`;
    return due.toLocaleDateString('de-CH');
}
function formatDoneLabel(value) {
    if (!value)
        return 'Erledigt';
    const done = new Date(value);
    const now = new Date();
    const isToday = done.getFullYear() === now.getFullYear() &&
        done.getMonth() === now.getMonth() &&
        done.getDate() === now.getDate();
    if (isToday)
        return 'Heute';
    return done.toLocaleDateString('de-CH');
}
exports.default = router;
