import { Request, Response, Router } from 'express';
import { pool } from '../../db/pool';
import { sendInternalServerError } from '../../middleware/error.middleware';
import { requireAuth } from '../../middleware/requireAuth';
import {
  completeMoveCaseTask,
  executeMoveCaseTaskAction,
  getMoveCaseTaskDetail,
  getMoveCaseTasks as getMoveCaseTasksService,
  saveMoveCaseTaskAnswers,
} from './move.services';
import { refreshMoveCaseTasks } from './move.task-sync';

const router = Router();

function logMoveEvent(
  event: string,
  req: Request,
  extra?: Record<string, unknown>
) {
  console.info(event, {
    route: `${req.method} ${req.originalUrl}`,
    userId: req.user?.id ?? null,
    ...extra,
  });
}

function logMoveError(
  event: string,
  req: Request,
  error: unknown,
  extra?: Record<string, unknown>
) {
  console.error(event, {
    route: `${req.method} ${req.originalUrl}`,
    userId: req.user?.id ?? null,
    ...extra,
    error: error instanceof Error ? error.message : String(error),
    stack: error instanceof Error ? error.stack ?? null : null,
  });
}

function getSingleParam(value: string | string[] | undefined): string | null {
  if (typeof value === 'string') return value;
  if (Array.isArray(value)) return value[0] ?? null;
  return null;
}

type MoveCaseRow = {
  id: string;
  user_id: string;
  from_city_id: string | null;
  to_city_id: string | null;
  move_date: string;
  has_car: boolean;
  has_children: boolean;
  has_dog: boolean;
  from_street: string | null;
  from_house_number: string | null;
  from_zip: string | null;
  to_street: string | null;
  to_house_number: string | null;
  to_zip: string | null;
  children_count: number | null;
  marital_status: string | null;
  health_insurance_name: string | null;
  employer_name: string | null;
  status: string;
  created_at: string;
  updated_at: string;
  from_city_name?: string | null;
  to_city_name?: string | null;
};

type MoveUserRow = {
  id: string;
  email: string;
  language: string | null;
  is_active: boolean | null;
};

type CityLookupRow = {
  id: string;
  name: string;
  canton_id: string | null;
};

type IdRow = {
  id: string;
};

function buildMoveCaseResponse(row: MoveCaseRow) {
  const missingFields: string[] = [];
  const missingFieldLabels = {
    de: [] as string[],
    fr: [] as string[],
    en: [] as string[],
  };

  const addMissing = (key: string, de: string, fr: string, en: string) => {
    missingFields.push(key);
    missingFieldLabels.de.push(de);
    missingFieldLabels.fr.push(fr);
    missingFieldLabels.en.push(en);
  };

  const checks: Array<{
    key: string;
    present: boolean;
    de: string;
    fr: string;
    en: string;
  }> = [
    {
      key: 'fromStreet',
      present: Boolean(row.from_street?.trim()),
      de: 'Alte Strasse',
      fr: 'Ancienne rue',
      en: 'Old street',
    },
    {
      key: 'fromHouseNumber',
      present: Boolean(row.from_house_number?.trim()),
      de: 'Alte Hausnummer',
      fr: 'Ancien numéro',
      en: 'Old house number',
    },
    {
      key: 'fromZip',
      present: Boolean(row.from_zip?.trim()),
      de: 'Alte PLZ',
      fr: 'Ancien NPA',
      en: 'Old ZIP code',
    },
    {
      key: 'toStreet',
      present: Boolean(row.to_street?.trim()),
      de: 'Neue Strasse',
      fr: 'Nouvelle rue',
      en: 'New street',
    },
    {
      key: 'toHouseNumber',
      present: Boolean(row.to_house_number?.trim()),
      de: 'Neue Hausnummer',
      fr: 'Nouveau numéro',
      en: 'New house number',
    },
    {
      key: 'toZip',
      present: Boolean(row.to_zip?.trim()),
      de: 'Neue PLZ',
      fr: 'Nouveau NPA',
      en: 'New ZIP code',
    },
  ];

  for (const check of checks) {
    if (!check.present) {
      addMissing(check.key, check.de, check.fr, check.en);
    }
  }

  const totalChecks = checks.length;
  const completedChecks = totalChecks - missingFields.length;
  const profileCompletenessPercent =
    totalChecks === 0 ? 100 : Math.round((completedChecks / totalChecks) * 100);

  return {
    ...row,
    profile_completeness_percent: profileCompletenessPercent,
    missing_fields: missingFields,
    missing_field_labels: missingFieldLabels,
  };
}

function normalizeSwissDateToIso(value: string): string | null {
  const trimmed = String(value).trim();

  if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
    return trimmed;
  }

  const match = trimmed.match(/^(\d{2})\.(\d{2})\.(\d{4})$/);
  if (!match) return null;

  const [, day, month, year] = match;
  return `${year}-${month}-${day}`;
}

async function tryRefreshMoveCaseTasks(caseId: string, source: string) {
  try {
    await refreshMoveCaseTasks(caseId);
    return true;
  } catch (error) {
    console.error(`[move] task refresh failed after ${source}`, {
      caseId,
      error: error instanceof Error ? error.message : String(error),
    });
    return false;
  }
}

router.get('/ping', (_req: Request, res: Response) => {
  res.status(200).json({ ok: true, route: 'move' });
});

router.post('/cases', requireAuth, async (req: Request, res: Response) => {
  const client = await pool.connect();
  let transactionOpen = false;

  try {
    const {
      fromCity,
      toCity,
      moveDate,
      hasCar,
      hasChildren,
      hasDog,
      fromStreet,
      fromHouseNumber,
      fromZip,
      toStreet,
      toHouseNumber,
      toZip,
      childrenCount,
      maritalStatus,
      healthInsuranceName,
      employerName,
    } = req.body ?? {};

    const userId = req.user?.id;

    if (!userId) {
      return res.status(401).json({
        message: 'Benutzer konnte nicht aus dem Token gelesen werden.',
      });
    }

    if (!fromCity || !toCity || !moveDate) {
      return res.status(400).json({
        message: 'fromCity, toCity und moveDate sind erforderlich.',
      });
    }

    const normalizedMoveDate = normalizeSwissDateToIso(String(moveDate));

    if (!normalizedMoveDate) {
      return res.status(400).json({
        message: 'Ungültiges Datum. Bitte verwende TT.MM.JJJJ oder JJJJ-MM-TT.',
      });
    }

    await client.query('BEGIN');
    transactionOpen = true;

    const userResult = await client.query<MoveUserRow>(
      `
      SELECT id, email, language, is_active
      FROM app.users
      WHERE id = $1
      LIMIT 1
      `,
      [userId]
    );

    if (!userResult.rows[0]) {
      await client.query('ROLLBACK');
      return res.status(401).json({
        message: 'Benutzer existiert nicht.',
      });
    }

    if (userResult.rows[0].is_active === false) {
      await client.query('ROLLBACK');
      return res.status(403).json({
        message: 'Benutzer ist nicht aktiv.',
      });
    }

    const fromCityResult = await client.query<CityLookupRow>(
      `
      SELECT id, name, canton_id
      FROM move_cities
      WHERE LOWER(name) = LOWER($1)
      LIMIT 1
      `,
      [String(fromCity).trim()]
    );

    const toCityResult = await client.query<CityLookupRow>(
      `
      SELECT id, name, canton_id
      FROM move_cities
      WHERE LOWER(name) = LOWER($1)
      LIMIT 1
      `,
      [String(toCity).trim()]
    );

    if (!fromCityResult.rows[0]) {
      await client.query('ROLLBACK');
      return res.status(400).json({
        message: `Startort nicht gefunden: ${fromCity}`,
      });
    }

    if (!toCityResult.rows[0]) {
      await client.query('ROLLBACK');
      return res.status(400).json({
        message: `Zielort nicht gefunden: ${toCity}`,
      });
    }

    const fromCityId = fromCityResult.rows[0].id;
    const toCityId = toCityResult.rows[0].id;

    const insertCaseResult = await client.query<MoveCaseRow>(
      `
      INSERT INTO move_cases (
        user_id,
        from_city_id,
        to_city_id,
        move_date,
        has_car,
        has_children,
        has_dog,
        from_street,
        from_house_number,
        from_zip,
        to_street,
        to_house_number,
        to_zip,
        children_count,
        marital_status,
        health_insurance_name,
        employer_name,
        status
      )
      VALUES (
        $1, $2, $3, $4, $5, $6, $7,
        $8, $9, $10, $11, $12, $13,
        $14, $15, $16, $17, $18
      )
      RETURNING
        id,
        user_id,
        from_city_id,
        to_city_id,
        move_date,
        has_car,
        has_children,
        has_dog,
        from_street,
        from_house_number,
        from_zip,
        to_street,
        to_house_number,
        to_zip,
        children_count,
        marital_status,
        health_insurance_name,
        employer_name,
        status,
        created_at,
        updated_at
      `,
      [
        userId,
        fromCityId,
        toCityId,
        normalizedMoveDate,
        hasCar ?? false,
        hasChildren ?? false,
        hasDog ?? false,
        fromStreet ?? null,
        fromHouseNumber ?? null,
        fromZip ?? null,
        toStreet ?? null,
        toHouseNumber ?? null,
        toZip ?? null,
        typeof childrenCount === 'number' ? childrenCount : null,
        maritalStatus ?? null,
        healthInsuranceName ?? null,
        employerName ?? null,
        'draft',
      ]
    );

    const createdCase = insertCaseResult.rows[0];

    await client.query('COMMIT');
    transactionOpen = false;

    const taskSyncOk = await tryRefreshMoveCaseTasks(createdCase.id, 'case-create');

    const refreshedCaseResult = await pool.query<MoveCaseRow>(
      `
      SELECT
        mc.id,
        mc.user_id,
        mc.from_city_id,
        mc.to_city_id,
        mc.move_date,
        mc.has_car,
        mc.has_children,
        mc.has_dog,
        mc.from_street,
        mc.from_house_number,
        mc.from_zip,
        mc.to_street,
        mc.to_house_number,
        mc.to_zip,
        mc.children_count,
        mc.marital_status,
        mc.health_insurance_name,
        mc.employer_name,
        mc.status,
        mc.created_at,
        mc.updated_at,
        fc.name AS from_city_name,
        tc.name AS to_city_name
      FROM move_cases mc
      LEFT JOIN move_cities fc ON fc.id = mc.from_city_id
      LEFT JOIN move_cities tc ON tc.id = mc.to_city_id
      WHERE mc.id = $1
      LIMIT 1
      `,
      [createdCase.id]
    );

    logMoveEvent('[MOVE_CASE_CREATE_OK]', req, {
      caseId: createdCase.id,
      taskSyncOk,
    });

    return res.status(201).json({
      case: buildMoveCaseResponse(refreshedCaseResult.rows[0] ?? createdCase),
      taskSyncOk,
    });
  } catch (error) {
    logMoveError('[MOVE_CASE_CREATE_ERROR]', req, error);
    if (transactionOpen) {
      await client.query('ROLLBACK');
    }
    return sendInternalServerError(res, error, 'Error creating move case:');
  } finally {
    client.release();
  }
});

router.get('/cases', requireAuth, async (req: Request, res: Response) => {
  try {
    const userId = req.user?.id;

    if (!userId) {
      return res.status(401).json({
        message: 'Benutzer konnte nicht aus dem Token gelesen werden.',
      });
    }

    const result = await pool.query<MoveCaseRow>(
      `
      SELECT
        mc.id,
        mc.user_id,
        mc.from_city_id,
        mc.to_city_id,
        mc.move_date,
        mc.has_car,
        mc.has_children,
        mc.has_dog,
        mc.from_street,
        mc.from_house_number,
        mc.from_zip,
        mc.to_street,
        mc.to_house_number,
        mc.to_zip,
        mc.children_count,
        mc.marital_status,
        mc.health_insurance_name,
        mc.employer_name,
        mc.status,
        mc.created_at,
        mc.updated_at,
        fc.name AS from_city_name,
        tc.name AS to_city_name
      FROM move_cases mc
      LEFT JOIN move_cities fc ON fc.id = mc.from_city_id
      LEFT JOIN move_cities tc ON tc.id = mc.to_city_id
      WHERE mc.user_id = $1
      ORDER BY
        CASE
          WHEN mc.status = 'draft' THEN 0
          WHEN mc.status = 'done' THEN 2
          ELSE 1
        END,
        mc.created_at DESC
      `,
      [userId]
    );

    return res.status(200).json({
      cases: result.rows.map((row) => buildMoveCaseResponse(row)),
    });
  } catch (error) {
    logMoveError('[MOVE_CASE_LIST_ERROR]', req, error);
    return sendInternalServerError(res, error, 'Error loading move cases:');
  }
});

router.get('/cases/current-active', requireAuth, async (req: Request, res: Response) => {
  try {
    const userId = req.user?.id;

    if (!userId) {
      return res.status(401).json({
        message: 'Benutzer konnte nicht aus dem Token gelesen werden.',
      });
    }

    const result = await pool.query<MoveCaseRow>(
      `
      SELECT
        mc.id,
        mc.user_id,
        mc.from_city_id,
        mc.to_city_id,
        mc.move_date,
        mc.has_car,
        mc.has_children,
        mc.has_dog,
        mc.from_street,
        mc.from_house_number,
        mc.from_zip,
        mc.to_street,
        mc.to_house_number,
        mc.to_zip,
        mc.children_count,
        mc.marital_status,
        mc.health_insurance_name,
        mc.employer_name,
        mc.status,
        mc.created_at,
        mc.updated_at,
        fc.name AS from_city_name,
        tc.name AS to_city_name
      FROM move_cases mc
      LEFT JOIN move_cities fc ON fc.id = mc.from_city_id
      LEFT JOIN move_cities tc ON tc.id = mc.to_city_id
      WHERE mc.user_id = $1
        AND mc.status = 'draft'
      ORDER BY mc.created_at DESC
      LIMIT 1
      `,
      [userId]
    );

    return res.status(200).json({
      case: result.rows[0] ? buildMoveCaseResponse(result.rows[0]) : null,
    });
  } catch (error) {
    logMoveError('[MOVE_CURRENT_ACTIVE_LOAD_ERROR]', req, error);
    return sendInternalServerError(
      res,
      error,
      'Error loading current active move case:'
    );
  }
});

router.get('/cases/:caseId', requireAuth, async (req: Request, res: Response) => {
  try {
    const caseId = getSingleParam(req.params.caseId);
    const userId = req.user?.id;

    if (!caseId) {
      return res.status(400).json({
        message: 'Case id ist erforderlich.',
      });
    }

    if (!userId) {
      return res.status(401).json({
        message: 'Benutzer konnte nicht aus dem Token gelesen werden.',
      });
    }

    const result = await pool.query<MoveCaseRow>(
      `
      SELECT
        mc.id,
        mc.user_id,
        mc.from_city_id,
        mc.to_city_id,
        mc.move_date,
        mc.has_car,
        mc.has_children,
        mc.has_dog,
        mc.from_street,
        mc.from_house_number,
        mc.from_zip,
        mc.to_street,
        mc.to_house_number,
        mc.to_zip,
        mc.children_count,
        mc.marital_status,
        mc.health_insurance_name,
        mc.employer_name,
        mc.status,
        mc.created_at,
        mc.updated_at,
        fc.name AS from_city_name,
        tc.name AS to_city_name
      FROM move_cases mc
      LEFT JOIN move_cities fc ON fc.id = mc.from_city_id
      LEFT JOIN move_cities tc ON tc.id = mc.to_city_id
      WHERE mc.id = $1
        AND mc.user_id = $2
      LIMIT 1
      `,
      [caseId, userId]
    );

    if (!result.rows[0]) {
      return res.status(404).json({
        message: 'Umzugsfall nicht gefunden.',
      });
    }

    logMoveEvent('[MOVE_CASE_LOAD_OK]', req, { caseId });

    return res.status(200).json({
      case: buildMoveCaseResponse(result.rows[0]),
    });
  } catch (error) {
    logMoveError('[MOVE_CASE_LOAD_ERROR]', req, error, {
      caseId: req.params.caseId ?? null,
    });
    return sendInternalServerError(res, error, 'Error loading move case:');
  }
});

router.patch('/cases/:caseId', requireAuth, async (req: Request, res: Response) => {
  try {
    const caseId = getSingleParam(req.params.caseId);
    const userId = req.user?.id;

    if (!caseId) {
      return res.status(400).json({
        message: 'Case id ist erforderlich.',
      });
    }

    if (!userId) {
      return res.status(401).json({
        message: 'Benutzer konnte nicht aus dem Token gelesen werden.',
      });
    }

    const caseCheck = await pool.query<MoveCaseRow>(
      `
      SELECT
        mc.id,
        mc.user_id,
        mc.from_city_id,
        mc.to_city_id,
        mc.move_date,
        mc.has_car,
        mc.has_children,
        mc.has_dog,
        mc.from_street,
        mc.from_house_number,
        mc.from_zip,
        mc.to_street,
        mc.to_house_number,
        mc.to_zip,
        mc.children_count,
        mc.marital_status,
        mc.health_insurance_name,
        mc.employer_name,
        mc.status,
        mc.created_at,
        mc.updated_at,
        fc.name AS from_city_name,
        tc.name AS to_city_name
      FROM move_cases mc
      LEFT JOIN move_cities fc ON fc.id = mc.from_city_id
      LEFT JOIN move_cities tc ON tc.id = mc.to_city_id
      WHERE mc.id = $1
        AND mc.user_id = $2
      LIMIT 1
      `,
      [caseId, userId]
    );

    if (!caseCheck.rows[0]) {
      return res.status(404).json({
        message: 'Umzugsfall nicht gefunden.',
      });
    }

    const currentCase = caseCheck.rows[0];

    const {
      fromStreet,
      fromHouseNumber,
      fromZip,
      toStreet,
      toHouseNumber,
      toZip,
      hasCar,
      hasChildren,
      hasDog,
      childrenCount,
      maritalStatus,
      healthInsuranceName,
      employerName,
    } = req.body ?? {};

    await pool.query(
      `
      UPDATE move_cases
      SET
        from_street = $1,
        from_house_number = $2,
        from_zip = $3,
        to_street = $4,
        to_house_number = $5,
        to_zip = $6,
        has_car = $7,
        has_children = $8,
        has_dog = $9,
        children_count = $10,
        marital_status = $11,
        health_insurance_name = $12,
        employer_name = $13,
        updated_at = NOW()
      WHERE id = $14
      `,
      [
        fromStreet ?? currentCase.from_street ?? null,
        fromHouseNumber ?? currentCase.from_house_number ?? null,
        fromZip ?? currentCase.from_zip ?? null,
        toStreet ?? currentCase.to_street ?? null,
        toHouseNumber ?? currentCase.to_house_number ?? null,
        toZip ?? currentCase.to_zip ?? null,
        typeof hasCar === 'boolean' ? hasCar : currentCase.has_car ?? false,
        typeof hasChildren === 'boolean'
          ? hasChildren
          : currentCase.has_children ?? false,
        typeof hasDog === 'boolean' ? hasDog : currentCase.has_dog ?? false,
        childrenCount ?? currentCase.children_count ?? null,
        maritalStatus ?? currentCase.marital_status ?? null,
        healthInsuranceName ?? currentCase.health_insurance_name ?? null,
        employerName ?? currentCase.employer_name ?? null,
        caseId,
      ]
    );

    const taskSyncOk = await tryRefreshMoveCaseTasks(caseId, 'case-update');

    const updatedCaseResult = await pool.query<MoveCaseRow>(
      `
      SELECT
        mc.id,
        mc.user_id,
        mc.from_city_id,
        mc.to_city_id,
        mc.move_date,
        mc.has_car,
        mc.has_children,
        mc.has_dog,
        mc.from_street,
        mc.from_house_number,
        mc.from_zip,
        mc.to_street,
        mc.to_house_number,
        mc.to_zip,
        mc.children_count,
        mc.marital_status,
        mc.health_insurance_name,
        mc.employer_name,
        mc.status,
        mc.created_at,
        mc.updated_at,
        fc.name AS from_city_name,
        tc.name AS to_city_name
      FROM move_cases mc
      LEFT JOIN move_cities fc ON fc.id = mc.from_city_id
      LEFT JOIN move_cities tc ON tc.id = mc.to_city_id
      WHERE mc.id = $1
      LIMIT 1
      `,
      [caseId]
    );

    return res.status(200).json({
      case: buildMoveCaseResponse(updatedCaseResult.rows[0]),
      taskSyncOk,
    });
  } catch (error) {
    logMoveError('[MOVE_CASE_UPDATE_ERROR]', req, error, {
      caseId: req.params.caseId ?? null,
    });
    return sendInternalServerError(res, error, 'Error updating move case:');
  }
});

router.get('/cases/:caseId/tasks', requireAuth, async (req: Request, res: Response) => {
  try {
    const caseId = getSingleParam(req.params.caseId);

    if (!caseId) {
      return res.status(400).json({
        message: 'Case id is required.',
      });
    }

    const userId = req.user?.id;

    if (!userId) {
      return res.status(401).json({
        message: 'Benutzer konnte nicht aus dem Token gelesen werden.',
      });
    }

    const caseCheck = await pool.query<IdRow>(
      `
      SELECT id
      FROM move_cases
      WHERE id = $1
        AND user_id = $2
      LIMIT 1
      `,
      [caseId, userId]
    );

    if (!caseCheck.rows[0]) {
      return res.status(404).json({
        message: 'Umzugsfall nicht gefunden.',
      });
    }

    const tasksResult = await getMoveCaseTasksService(caseId, userId);

    logMoveEvent('[MOVE_TASK_LIST_OK]', req, {
      caseId,
      taskCount: tasksResult.tasks.length,
    });

    return res.status(200).json({
      tasks: tasksResult.tasks,
    });
  } catch (error) {
    logMoveError('[MOVE_TASK_LIST_ERROR]', req, error, {
      caseId: req.params.caseId ?? null,
    });
    return sendInternalServerError(res, error, 'Error loading move tasks:');
  }
});

router.get(
  '/cases/:caseId/tasks/:taskId',
  requireAuth,
  async (req: Request, res: Response) => {
    try {
      const caseId = getSingleParam(req.params.caseId);
      const taskId = getSingleParam(req.params.taskId);
      const userId = req.user?.id;

      if (!caseId || !taskId) {
        return res.status(400).json({
          message: 'Case id und task id sind erforderlich.',
        });
      }

      if (!userId) {
        return res.status(401).json({
          message: 'Benutzer konnte nicht aus dem Token gelesen werden.',
        });
      }

      const detail = await getMoveCaseTaskDetail(caseId, taskId, userId);

      logMoveEvent('[MOVE_TASK_DETAIL_OK]', req, {
        caseId,
        taskId,
        nextTaskId: detail.nextTaskId,
      });

      return res.status(200).json(detail);
    } catch (error) {
      logMoveError('[MOVE_TASK_DETAIL_ERROR]', req, error, {
        caseId: req.params.caseId ?? null,
        taskId: req.params.taskId ?? null,
      });
      if (error instanceof Error) {
        if (error.message === 'Move case not found') {
          return res.status(404).json({
            message: 'Umzugsfall nicht gefunden.',
          });
        }

        if (error.message === 'Move task not found') {
          return res.status(404).json({
            message: 'Aufgabe nicht gefunden.',
          });
        }
      }

      return sendInternalServerError(res, error, 'Error loading move task:');
    }
  }
);

router.put(
  '/cases/:caseId/tasks/:taskId/answers',
  requireAuth,
  async (req: Request, res: Response) => {
    try {
      const caseId = getSingleParam(req.params.caseId);
      const taskId = getSingleParam(req.params.taskId);
      const userId = req.user?.id;
      const answers = Array.isArray(req.body?.answers) ? req.body.answers : [];

      if (!caseId || !taskId) {
        return res.status(400).json({
          message: 'Case id und task id sind erforderlich.',
        });
      }

      if (!userId) {
        return res.status(401).json({
          message: 'Benutzer konnte nicht aus dem Token gelesen werden.',
        });
      }

      const detail = await saveMoveCaseTaskAnswers(caseId, taskId, userId, answers);

      logMoveEvent('[MOVE_TASK_ANSWERS_SAVE_OK]', req, {
        caseId,
        taskId,
        answerCount: answers.length,
        nextTaskId: detail.nextTaskId,
      });

      return res.status(200).json(detail);
    } catch (error) {
      logMoveError('[MOVE_TASK_ANSWERS_SAVE_ERROR]', req, error, {
        caseId: req.params.caseId ?? null,
        taskId: req.params.taskId ?? null,
      });
      if (error instanceof Error) {
        if (error.message === 'Answers are required') {
          return res.status(400).json({
            message: 'Mindestens eine Antwort ist erforderlich.',
          });
        }

        if (error.message === 'Move task not found') {
          return res.status(404).json({
            message: 'Aufgabe nicht gefunden.',
          });
        }
      }

      return sendInternalServerError(res, error, 'Error saving move task answers:');
    }
  }
);

router.post(
  '/cases/:caseId/tasks/:taskId/actions/:actionType',
  requireAuth,
  async (req: Request, res: Response) => {
    try {
      const caseId = getSingleParam(req.params.caseId);
      const taskId = getSingleParam(req.params.taskId);
      const actionType = getSingleParam(req.params.actionType);
      const userId = req.user?.id;

      if (!caseId || !taskId || !actionType) {
        return res.status(400).json({
          message: 'Case id, task id und action type sind erforderlich.',
        });
      }

      if (!userId) {
        return res.status(401).json({
          message: 'Benutzer konnte nicht aus dem Token gelesen werden.',
        });
      }

      const result = await executeMoveCaseTaskAction(
        caseId,
        taskId,
        actionType,
        userId,
        req.body?.language ?? req.query.language
      );

      logMoveEvent('[MOVE_TASK_ACTION_OK]', req, {
        caseId,
        taskId,
        actionType,
        nextTaskId: result.nextTaskId,
      });

      return res.status(200).json(result);
    } catch (error) {
      logMoveError('[MOVE_TASK_ACTION_ERROR]', req, error, {
        caseId: req.params.caseId ?? null,
        taskId: req.params.taskId ?? null,
        actionType: req.params.actionType ?? null,
      });
      if (error instanceof Error) {
        if (error.message === 'Unsupported move task action') {
          return res.status(400).json({
            message: 'Action-Type wird nicht unterstützt.',
          });
        }

        if (
          error.message === 'Move task not found' ||
          error.message === 'Move case not found'
        ) {
          return res.status(404).json({
            message: 'Aufgabe nicht gefunden.',
          });
        }

        if (
          error.message === 'Move task action not available' ||
          error.message === 'Move task action payload invalid'
        ) {
          return res.status(400).json({
            message: 'Die gewünschte Aktion ist für diese Aufgabe nicht verfügbar.',
          });
        }
      }

      return sendInternalServerError(res, error, 'Error executing move task action:');
    }
  }
);

router.patch(
  '/cases/:caseId/tasks/:taskId/complete',
  requireAuth,
  async (req: Request, res: Response) => {
    try {
      const caseId = getSingleParam(req.params.caseId);
      const taskId = getSingleParam(req.params.taskId);
      const userId = req.user?.id;

      if (!caseId || !taskId) {
        return res.status(400).json({
          message: 'Case id und task id sind erforderlich.',
        });
      }

      if (!userId) {
        return res.status(401).json({
          message: 'Benutzer konnte nicht aus dem Token gelesen werden.',
        });
      }

      const result = await completeMoveCaseTask(caseId, taskId, userId);

      logMoveEvent('[MOVE_TASK_COMPLETE_OK]', req, {
        caseId,
        taskId,
        nextTaskId: result.nextTaskId,
      });

      return res.status(200).json({
        task: result.task,
        nextTaskId: result.nextTaskId,
      });
    } catch (error) {
      logMoveError('[MOVE_TASK_COMPLETE_ERROR]', req, error, {
        caseId: req.params.caseId ?? null,
        taskId: req.params.taskId ?? null,
      });
      if (error instanceof Error) {
        if (
          error.message === 'Move task not found' ||
          error.message === 'Move case not found'
        ) {
          return res.status(404).json({
            message: 'Aufgabe nicht gefunden.',
          });
        }
      }

      return sendInternalServerError(
        res,
        error,
        'Error completing move task:'
      );
    }
  }
);

export default router;
