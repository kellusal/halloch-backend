import { Request, Response, Router } from 'express';
import { pool } from '../../db/pool';
import { sendInternalServerError } from '../../middleware/error.middleware';
import { requireAuth } from '../../middleware/requireAuth';
import { refreshMoveCaseTasks } from './move.task-sync';

const router = Router();

type MoveCaseRow = {
  id: string;
  user_id: string;
  from_city_id: string;
  to_city_id: string;
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

    await refreshMoveCaseTasks(createdCase.id);

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

    return res.status(201).json({
      case: buildMoveCaseResponse(refreshedCaseResult.rows[0] ?? createdCase),
    });
  } catch (error) {
    if (transactionOpen) {
      await client.query('ROLLBACK');
    }
    return sendInternalServerError(res, error, 'Error creating move case:');
  } finally {
    client.release();
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
        AND mc.status IN ('draft', 'in_progress')
      ORDER BY mc.created_at DESC
      LIMIT 1
      `,
      [userId]
    );

    return res.status(200).json({
      case: result.rows[0] ? buildMoveCaseResponse(result.rows[0]) : null,
    });
  } catch (error) {
    return sendInternalServerError(
      res,
      error,
      'Error loading current active move case:'
    );
  }
});

router.get('/cases/:caseId', requireAuth, async (req: Request, res: Response) => {
  try {
    const { caseId } = req.params;
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

    return res.status(200).json({
      case: buildMoveCaseResponse(result.rows[0]),
    });
  } catch (error) {
    return sendInternalServerError(res, error, 'Error loading move case:');
  }
});

router.patch('/cases/:caseId', requireAuth, async (req: Request, res: Response) => {
  try {
    const { caseId } = req.params;
    const userId = req.user?.id;

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

    await refreshMoveCaseTasks(caseId as string);

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
    });
  } catch (error) {
    return sendInternalServerError(res, error, 'Error updating move case:');
  }
});

router.get('/cases/:caseId/tasks', requireAuth, async (req: Request, res: Response) => {
  try {
    const rawCaseId = req.params.caseId;

    const caseId =
      typeof rawCaseId === 'string' ? rawCaseId : rawCaseId?.[0];

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

    const tasksResult = await pool.query(
      `
      SELECT
        id,
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
        action_type,
        action_payload,
        secondary_action_type,
        secondary_action_payload,
        action_status,
        completed_at,
        created_at,
        updated_at
      FROM move_case_tasks
      WHERE case_id = $1
      ORDER BY sort_order ASC, created_at ASC
      `,
      [caseId]
    );

    return res.status(200).json({
      tasks: tasksResult.rows,
    });
  } catch (error) {
    return sendInternalServerError(res, error, 'Error loading move tasks:');
  }
});

router.get(
  '/cases/:caseId/tasks/:taskId',
  requireAuth,
  async (req: Request, res: Response) => {
    try {
      const { caseId, taskId } = req.params;
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

      const taskResult = await pool.query(
        `
        SELECT
          id,
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
          action_type,
          action_payload,
          secondary_action_type,
          secondary_action_payload,
          action_status,
          completed_at,
          created_at,
          updated_at
        FROM move_case_tasks
        WHERE id = $1
          AND case_id = $2
        LIMIT 1
        `,
        [taskId, caseId]
      );

      if (!taskResult.rows[0]) {
        return res.status(404).json({
          message: 'Aufgabe nicht gefunden.',
        });
      }

      const nextTaskResult = await pool.query<IdRow>(
        `
        SELECT id
        FROM move_case_tasks
        WHERE case_id = $1
          AND status = 'open'
          AND (
            sort_order > COALESCE(
              (SELECT sort_order FROM move_case_tasks WHERE id = $2),
              -1
            )
            OR (
              sort_order = COALESCE(
                (SELECT sort_order FROM move_case_tasks WHERE id = $2),
                -1
              )
              AND id <> $2
            )
          )
        ORDER BY sort_order ASC, created_at ASC
        LIMIT 1
        `,
        [caseId, taskId]
      );

      return res.status(200).json({
        task: taskResult.rows[0],
        nextTaskId: nextTaskResult.rows[0]?.id ?? null,
      });
    } catch (error) {
      return sendInternalServerError(res, error, 'Error loading move task:');
    }
  }
);

router.patch(
  '/cases/:caseId/tasks/:taskId/complete',
  requireAuth,
  async (req: Request, res: Response) => {
    try {
      const { caseId, taskId } = req.params;
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

      const updateResult = await pool.query(
        `
        UPDATE move_case_tasks
        SET
          status = 'done',
          completed_at = NOW(),
          updated_at = NOW()
        WHERE id = $1
          AND case_id = $2
        RETURNING
          id,
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
          action_type,
          action_payload,
          secondary_action_type,
          secondary_action_payload,
          action_status,
          completed_at,
          created_at,
          updated_at
        `,
        [taskId, caseId]
      );

      if (!updateResult.rows[0]) {
        return res.status(404).json({
          message: 'Aufgabe nicht gefunden.',
        });
      }

      await refreshMoveCaseTasks(caseId as string);

      const nextTaskResult = await pool.query<IdRow>(
        `
        SELECT id
        FROM move_case_tasks
        WHERE case_id = $1
          AND status = 'open'
        ORDER BY sort_order ASC, created_at ASC
        LIMIT 1
        `,
        [caseId]
      );

      return res.status(200).json({
        task: updateResult.rows[0],
        nextTaskId: nextTaskResult.rows[0]?.id ?? null,
      });
    } catch (error) {
      return sendInternalServerError(
        res,
        error,
        'Error completing move task:'
      );
    }
  }
);

export default router;
