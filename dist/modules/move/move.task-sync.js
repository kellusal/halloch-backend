"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.refreshMoveCaseTasks = refreshMoveCaseTasks;
exports.refreshActiveMoveCasesForUser = refreshActiveMoveCasesForUser;
const pool_1 = require("../../db/pool");
function normalizeLanguageCode(value) {
    const trimmed = String(value ?? 'de').trim().toLowerCase();
    if (trimmed.startsWith('fr'))
        return 'fr';
    if (trimmed.startsWith('en'))
        return 'en';
    return 'de';
}
async function refreshMoveCaseTasks(caseId) {
    const client = await pool_1.pool.connect();
    try {
        await client.query('BEGIN');
        const contextResult = await client.query(`
      SELECT
        mc.id AS case_id,
        mc.user_id,
        u.language,
        mc.to_city_id,
        tc.canton_id AS to_canton_id,
        mc.move_date,

        COALESCE(up.has_car, mc.has_car, FALSE) AS has_car,
        COALESCE(up.has_children, mc.has_children, FALSE) AS has_children,
        COALESCE(up.has_dog, mc.has_dog, FALSE) AS has_dog,
        COALESCE(up.children_count, mc.children_count) AS children_count,
        COALESCE(u.marital_status, mc.marital_status) AS marital_status,
        COALESCE(up.health_insurance_name, mc.health_insurance_name) AS health_insurance_name,
        COALESCE(up.employer_name, mc.employer_name) AS employer_name
      FROM move_cases mc
      INNER JOIN app.users u
        ON u.id = mc.user_id
      INNER JOIN move_cities tc
        ON tc.id = mc.to_city_id
      LEFT JOIN app.user_profiles up
        ON up.user_id = mc.user_id
      WHERE mc.id = $1
      LIMIT 1
      `, [caseId]);
        if (!contextResult.rows[0]) {
            throw new Error('Move case not found');
        }
        const ctx = contextResult.rows[0];
        const language = normalizeLanguageCode(ctx.language);
        await client.query(`
      UPDATE move_cases
      SET
        has_car = $1,
        has_children = $2,
        has_dog = $3,
        children_count = $4,
        marital_status = $5,
        health_insurance_name = $6,
        employer_name = $7,
        updated_at = NOW()
      WHERE id = $8
      `, [
            Boolean(ctx.has_car),
            Boolean(ctx.has_children),
            Boolean(ctx.has_dog),
            ctx.children_count ?? null,
            ctx.marital_status ?? null,
            ctx.health_insurance_name ?? null,
            ctx.employer_name ?? null,
            caseId,
        ]);
        const relevantTemplatesResult = await client.query(`
      SELECT
        t.id AS template_id,
        t.category,

        CASE
          WHEN $1 = 'fr' THEN COALESCE(NULLIF(t.header_fr, ''), NULLIF(t.header_de, ''), NULLIF(t.header_en, ''))
          WHEN $1 = 'en' THEN COALESCE(NULLIF(t.header_en, ''), NULLIF(t.header_de, ''), NULLIF(t.header_fr, ''))
          ELSE COALESCE(NULLIF(t.header_de, ''), NULLIF(t.header_fr, ''), NULLIF(t.header_en, ''))
        END AS header,

        NULLIF(t.header_de, '') AS header_de,
        NULLIF(t.header_fr, '') AS header_fr,
        NULLIF(t.header_en, '') AS header_en,

        CASE
          WHEN $1 = 'fr' THEN COALESCE(NULLIF(t.title_fr, ''), NULLIF(t.title_de, ''), NULLIF(t.title_en, ''), 'Tâche')
          WHEN $1 = 'en' THEN COALESCE(NULLIF(t.title_en, ''), NULLIF(t.title_de, ''), NULLIF(t.title_fr, ''), 'Task')
          ELSE COALESCE(NULLIF(t.title_de, ''), NULLIF(t.title_fr, ''), NULLIF(t.title_en, ''), 'Aufgabe')
        END AS title,

        NULLIF(t.title_de, '') AS title_de,
        NULLIF(t.title_fr, '') AS title_fr,
        NULLIF(t.title_en, '') AS title_en,

        CASE
          WHEN $1 = 'fr' THEN COALESCE(NULLIF(t.description_fr, ''), NULLIF(t.description_de, ''), NULLIF(t.description_en, ''))
          WHEN $1 = 'en' THEN COALESCE(NULLIF(t.description_en, ''), NULLIF(t.description_de, ''), NULLIF(t.description_fr, ''))
          ELSE COALESCE(NULLIF(t.description_de, ''), NULLIF(t.description_fr, ''), NULLIF(t.description_en, ''))
        END AS description,

        NULLIF(t.description_de, '') AS description_de,
        NULLIF(t.description_fr, '') AS description_fr,
        NULLIF(t.description_en, '') AS description_en,

        CASE
          WHEN t.trigger_before_move_days IS NOT NULL THEN ($2::date - (t.trigger_before_move_days * INTERVAL '1 day'))::date
          WHEN t.trigger_after_move_days IS NOT NULL THEN ($2::date + (t.trigger_after_move_days * INTERVAL '1 day'))::date
          ELSE NULL
        END AS due_date,

        t.sort_order,

        link_pick.url AS external_url,

        CASE
          WHEN $1 = 'fr' THEN COALESCE(NULLIF(link_pick.label_fr, ''), NULLIF(link_pick.label_de, ''), NULLIF(link_pick.label_en, ''))
          WHEN $1 = 'en' THEN COALESCE(NULLIF(link_pick.label_en, ''), NULLIF(link_pick.label_de, ''), NULLIF(link_pick.label_fr, ''))
          ELSE COALESCE(NULLIF(link_pick.label_de, ''), NULLIF(link_pick.label_fr, ''), NULLIF(link_pick.label_en, ''))
        END AS link_label,

        TRUE AS is_required,

        CASE
          WHEN link_pick.city_id IS NOT NULL OR link_pick.canton_id IS NOT NULL THEN TRUE
          ELSE FALSE
        END AS is_city_specific,

        t.action_type,
        t.action_payload,
        t.secondary_action_type,
        t.secondary_action_payload

      FROM move_task_templates t

      LEFT JOIN LATERAL (
        SELECT
          l.id,
          l.city_id,
          l.canton_id,
          l.label_de,
          l.label_fr,
          l.label_en,
          l.url,
          l.sort_order
        FROM move_task_template_links l
        WHERE l.task_template_id = t.id
          AND l.is_active = TRUE
          AND (l.language_code IS NULL OR LOWER(l.language_code) = LOWER($1))
          AND (
            l.city_id = $3
            OR (l.city_id IS NULL AND l.canton_id = $4)
            OR (l.city_id IS NULL AND l.canton_id IS NULL)
          )
        ORDER BY
          CASE
            WHEN l.city_id = $3 THEN 1
            WHEN l.city_id IS NULL AND l.canton_id = $4 THEN 2
            ELSE 3
          END,
          l.sort_order ASC,
          l.created_at ASC
        LIMIT 1
      ) AS link_pick ON TRUE

      WHERE t.is_active = TRUE
        AND (COALESCE(t.requires_car, FALSE) = FALSE OR $5 = TRUE)
        AND (COALESCE(t.requires_children, FALSE) = FALSE OR $6 = TRUE)
        AND (COALESCE(t.requires_dog, FALSE) = FALSE OR $7 = TRUE)

      ORDER BY t.sort_order ASC, t.created_at ASC
      `, [
            language, // $1
            ctx.move_date, // $2
            ctx.to_city_id, // $3
            ctx.to_canton_id, // $4
            Boolean(ctx.has_car), // $5
            Boolean(ctx.has_children), // $6
            Boolean(ctx.has_dog), // $7
        ]);
        const relevantTemplates = relevantTemplatesResult.rows;
        const existingTasksResult = await client.query(`
      SELECT
        id,
        template_id,
        status
      FROM move_case_tasks
      WHERE case_id = $1
        AND template_id IS NOT NULL
      `, [caseId]);
        const existingTasks = existingTasksResult.rows;
        const doneTemplateIds = new Set(existingTasks
            .filter((task) => task.status === 'done' && task.template_id)
            .map((task) => String(task.template_id)));
        const openTaskByTemplateId = new Map();
        for (const task of existingTasks) {
            if (task.status === 'open' && task.template_id) {
                openTaskByTemplateId.set(String(task.template_id), task);
            }
        }
        const relevantTemplateIds = new Set(relevantTemplates.map((template) => String(template.template_id)));
        for (const task of existingTasks) {
            if (task.status === 'open' &&
                task.template_id &&
                !relevantTemplateIds.has(String(task.template_id))) {
                await client.query(`
          DELETE FROM move_case_tasks
          WHERE id = $1
            AND case_id = $2
            AND status = 'open'
          `, [task.id, caseId]);
            }
        }
        for (const template of relevantTemplates) {
            const templateId = String(template.template_id);
            if (doneTemplateIds.has(templateId)) {
                continue;
            }
            const existingOpenTask = openTaskByTemplateId.get(templateId);
            if (existingOpenTask) {
                await client.query(`
          UPDATE move_case_tasks
          SET
            category = $1,

            header = $2,
            header_de = $3,
            header_fr = $4,
            header_en = $5,

            title = $6,
            title_de = $7,
            title_fr = $8,
            title_en = $9,

            description = $10,
            description_de = $11,
            description_fr = $12,
            description_en = $13,

            due_date = $14,
            sort_order = $15,
            external_url = $16,
            link_label = $17,
            is_required = $18,
            is_city_specific = $19,
            action_type = $20,
            action_payload = $21,
            secondary_action_type = $22,
            secondary_action_payload = $23,
            updated_at = NOW()
          WHERE id = $24
          `, [
                    template.category,
                    template.header,
                    template.header_de,
                    template.header_fr,
                    template.header_en,
                    template.title,
                    template.title_de,
                    template.title_fr,
                    template.title_en,
                    template.description,
                    template.description_de,
                    template.description_fr,
                    template.description_en,
                    template.due_date,
                    template.sort_order,
                    template.external_url,
                    template.link_label,
                    template.is_required,
                    template.is_city_specific,
                    template.action_type,
                    template.action_payload,
                    template.secondary_action_type,
                    template.secondary_action_payload,
                    existingOpenTask.id,
                ]);
            }
            else {
                await client.query(`
          INSERT INTO move_case_tasks (
            case_id,
            template_id,
            city_service_id,
            category,

            header,
            header_de,
            header_fr,
            header_en,

            title,
            title_de,
            title_fr,
            title_en,

            description,
            description_de,
            description_fr,
            description_en,

            status,
            due_date,
            sort_order,
            external_url,
            link_label,
            is_required,
            is_city_specific,
            completed_at,
            action_type,
            action_payload,
            secondary_action_type,
            secondary_action_payload,
            action_status
          )
          VALUES (
            $1, $2, NULL, $3,
            $4, $5, $6, $7,
            $8, $9, $10, $11,
            $12, $13, $14, $15,
            'open', $16, $17, $18, $19, $20, $21, NULL,
            $22, $23, $24, $25, 'pending'
          )
          `, [
                    caseId,
                    template.template_id,
                    template.category,
                    template.header,
                    template.header_de,
                    template.header_fr,
                    template.header_en,
                    template.title,
                    template.title_de,
                    template.title_fr,
                    template.title_en,
                    template.description,
                    template.description_de,
                    template.description_fr,
                    template.description_en,
                    template.due_date,
                    template.sort_order,
                    template.external_url,
                    template.link_label,
                    template.is_required,
                    template.is_city_specific,
                    template.action_type,
                    template.action_payload,
                    template.secondary_action_type,
                    template.secondary_action_payload,
                ]);
            }
        }
        await client.query('COMMIT');
    }
    catch (error) {
        await client.query('ROLLBACK');
        throw error;
    }
    finally {
        client.release();
    }
}
async function refreshActiveMoveCasesForUser(userId) {
    const result = await pool_1.pool.query(`
    SELECT id
    FROM move_cases
    WHERE user_id = $1
      AND status IN ('draft', 'in_progress')
    ORDER BY created_at DESC
    `, [userId]);
    for (const row of result.rows) {
        await refreshMoveCaseTasks(String(row.id));
    }
}
