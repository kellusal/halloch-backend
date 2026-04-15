"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.createMoveCase = createMoveCase;
exports.getMoveCaseById = getMoveCaseById;
exports.getMoveCaseTasks = getMoveCaseTasks;
const pool_1 = require("../../db/pool");
const move_generator_1 = require("./move.generator");
function mapMoveCase(row) {
    return {
        id: row.id,
        userId: row.user_id,
        fromCityId: row.from_city_id,
        toCityId: row.to_city_id,
        moveDate: row.move_date,
        hasCar: row.has_car,
        hasChildren: row.has_children,
        status: row.status,
        createdAt: row.created_at,
        updatedAt: row.updated_at,
    };
}
function mapMoveCaseTask(row) {
    return {
        id: row.id,
        caseId: row.case_id,
        templateId: row.template_id,
        cityServiceId: row.city_service_id,
        category: row.category,
        title: row.title,
        description: row.description,
        titleDe: row.title_de,
        titleFr: row.title_fr,
        titleEn: row.title_en,
        descriptionDe: row.description_de,
        descriptionFr: row.description_fr,
        descriptionEn: row.description_en,
        status: row.status,
        dueDate: row.due_date,
        sortOrder: row.sort_order,
        externalUrl: row.external_url,
        linkLabel: row.link_label,
        isRequired: row.is_required,
        isCitySpecific: row.is_city_specific,
        actionType: row.action_type,
        actionPayload: row.action_payload,
        secondaryActionType: row.secondary_action_type,
        secondaryActionPayload: row.secondary_action_payload,
        actionStatus: row.action_status,
        completedAt: row.completed_at,
        createdAt: row.created_at,
        updatedAt: row.updated_at,
    };
}
async function findCityByName(cityName) {
    const value = cityName.trim();
    if (!value) {
        return null;
    }
    const result = await pool_1.pool.query(`
      SELECT id, name
      FROM public.move_cities
      WHERE LOWER(name) = LOWER($1)
      LIMIT 1
    `, [value]);
    if (!result.rowCount) {
        return null;
    }
    return result.rows[0];
}
async function createMoveCase(input) {
    if (!input.userId) {
        throw new Error('User is required');
    }
    if (!input.toCity?.trim()) {
        throw new Error('To city is required');
    }
    if (!input.moveDate?.trim()) {
        throw new Error('Move date is required');
    }
    const fromCity = input.fromCity?.trim()
        ? await findCityByName(input.fromCity)
        : null;
    const toCity = await findCityByName(input.toCity);
    if (!toCity) {
        throw new Error('Destination city not found');
    }
    if (input.fromCity?.trim() && !fromCity) {
        throw new Error('Origin city not found');
    }
    const insertResult = await pool_1.pool.query(`
      INSERT INTO public.move_cases (
        user_id,
        from_city_id,
        to_city_id,
        move_date,
        has_car,
        has_children,
        status
      )
      VALUES ($1, $2, $3, $4, $5, $6, 'in_progress')
      RETURNING
        id,
        user_id,
        from_city_id,
        to_city_id,
        move_date,
        has_car,
        has_children,
        status,
        created_at,
        updated_at
    `, [
        input.userId,
        fromCity?.id ?? null,
        toCity.id,
        input.moveDate,
        input.hasCar,
        input.hasChildren,
    ]);
    const createdCase = insertResult.rows[0];
    await (0, move_generator_1.generateTasksForCase)(createdCase.id);
    return {
        case: mapMoveCase(createdCase),
    };
}
async function getMoveCaseById(caseId, userId) {
    if (!caseId?.trim()) {
        throw new Error('Case id is required');
    }
    const result = await pool_1.pool.query(`
      SELECT
        id,
        user_id,
        from_city_id,
        to_city_id,
        move_date,
        has_car,
        has_children,
        status,
        created_at,
        updated_at
      FROM public.move_cases
      WHERE id = $1
        AND user_id = $2
      LIMIT 1
    `, [caseId, userId]);
    if (!result.rowCount) {
        throw new Error('Move case not found');
    }
    return {
        case: mapMoveCase(result.rows[0]),
    };
}
async function getMoveCaseTasks(caseId, userId) {
    if (!caseId?.trim()) {
        throw new Error('Case id is required');
    }
    const ownershipResult = await pool_1.pool.query(`
      SELECT id
      FROM public.move_cases
      WHERE id = $1
        AND user_id = $2
      LIMIT 1
    `, [caseId, userId]);
    if (!ownershipResult.rowCount) {
        throw new Error('Move case not found');
    }
    const result = await pool_1.pool.query(`
      SELECT
        id,
        case_id,
        template_id,
        city_service_id,
        category,

        title,
        description,

        title_de,
        title_fr,
        title_en,

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

        action_type,
        action_payload,
        secondary_action_type,
        secondary_action_payload,
        action_status,

        completed_at,
        created_at,
        updated_at
      FROM public.move_case_tasks
      WHERE case_id = $1
      ORDER BY sort_order ASC, created_at ASC
    `, [caseId]);
    return {
        tasks: result.rows.map(mapMoveCaseTask),
    };
}
