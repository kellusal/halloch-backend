import { pool } from '../../db/pool';

type MoveCaseForGeneration = {
  id: string;
  to_city_id: string;
  move_date: string;
  has_car: boolean;
  has_children: boolean;
};

type TemplateRow = {
  id: string;
  category: string;

  title_de: string | null;
  title_fr: string | null;
  title_en: string | null;

  description_de: string | null;
  description_fr: string | null;
  description_en: string | null;

  sort_order: number | null;
  is_active: boolean | null;
  requires_car: boolean | null;
  requires_children: boolean | null;

  trigger_before_move_days: number | null;
  trigger_after_move_days: number | null;

  action_type: string | null;
  action_payload: any | null;
  secondary_action_type: string | null;
  secondary_action_payload: any | null;
};

type TemplateLinkRow = {
  task_template_id: string;
  label_de: string | null;
  label_fr: string | null;
  label_en: string | null;
  url: string | null;
};

type CityServiceRow = {
  id: string;
  city_id: string;
  category: string;
  title: string;
  description: string | null;
  url: string | null;
  sort_order: number | null;
  is_active: boolean | null;
};

function addDays(dateString: string, days: number) {
  const date = new Date(dateString);
  date.setDate(date.getDate() + days);
  return date.toISOString().slice(0, 10);
}

function getDefaultLocalizedTitle(template: TemplateRow) {
  return (
    template.title_de ||
    template.title_fr ||
    template.title_en ||
    'Aufgabe'
  );
}

function getDefaultLocalizedDescription(template: TemplateRow) {
  return (
    template.description_de ||
    template.description_fr ||
    template.description_en ||
    null
  );
}

function getDefaultLinkLabel(link?: TemplateLinkRow) {
  return link?.label_de || link?.label_fr || link?.label_en || null;
}

export async function generateTasksForCase(caseId: string) {
  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    const caseResult = await client.query<MoveCaseForGeneration>(
      `
        SELECT
          id,
          to_city_id,
          move_date,
          has_car,
          has_children
        FROM public.move_cases
        WHERE id = $1
        LIMIT 1
      `,
      [caseId]
    );

    if (!caseResult.rowCount || caseResult.rowCount === 0) {
      throw new Error('Move case not found');
    }

    const moveCase = caseResult.rows[0];

    const templateResult = await client.query<TemplateRow>(
      `
        SELECT
          id,
          category,

          title_de,
          title_fr,
          title_en,

          description_de,
          description_fr,
          description_en,

          sort_order,
          is_active,
          requires_car,
          requires_children,

          trigger_before_move_days,
          trigger_after_move_days,

          action_type,
          action_payload,
          secondary_action_type,
          secondary_action_payload
        FROM public.move_task_templates
        WHERE COALESCE(is_active, true) = true
        ORDER BY COALESCE(sort_order, 0), created_at, id
      `
    );

    const templates = templateResult.rows.filter((template: { requires_car: boolean; requires_children: boolean; }) => {
      if (template.requires_car === true && !moveCase.has_car) {
        return false;
      }

      if (template.requires_children === true && !moveCase.has_children) {
        return false;
      }

      return true;
    });

    const templateIds = templates.map((template: { id: any; }) => template.id);

    let linksByTemplateId = new Map<string, TemplateLinkRow>();

    if (templateIds.length > 0) {
      const linkResult = await client.query<TemplateLinkRow>(
        `
          SELECT
            task_template_id,
            label_de,
            label_fr,
            label_en,
            url
          FROM public.move_task_template_links
          WHERE task_template_id = ANY($1::uuid[])
            AND COALESCE(is_active, true) = true
        `,
        [templateIds]
      );

      linksByTemplateId = new Map(
        linkResult.rows.map((row: { task_template_id: any; }) => [row.task_template_id, row])
      );
    }

    for (const template of templates) {
      const link = linksByTemplateId.get(template.id);

      let dueDate: string | null = null;

      if (template.trigger_before_move_days !== null) {
        dueDate = addDays(moveCase.move_date, -template.trigger_before_move_days);
      } else if (template.trigger_after_move_days !== null) {
        dueDate = addDays(moveCase.move_date, template.trigger_after_move_days);
      }

      await client.query(
        `
          INSERT INTO public.move_case_tasks (
            case_id,
            template_id,
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
            action_status
          )
          VALUES (
            $1, $2, $3,
            $4, $5,
            $6, $7, $8,
            $9, $10, $11,
            'open', $12, $13, $14, $15, $16, false,
            $17, $18, $19, $20, 'pending'
          )
        `,
        [
          moveCase.id,
          template.id,
          template.category,

          getDefaultLocalizedTitle(template),
          getDefaultLocalizedDescription(template),

          template.title_de,
          template.title_fr,
          template.title_en,

          template.description_de,
          template.description_fr,
          template.description_en,

          dueDate,
          template.sort_order ?? 0,
          link?.url ?? template.action_payload?.url ?? null,
          getDefaultLinkLabel(link) ?? template.action_payload?.label?.de ?? null,
          true,

          template.action_type,
          template.action_payload,
          template.secondary_action_type,
          template.secondary_action_payload,
        ]
      );
    }

    const cityServiceResult = await client.query<CityServiceRow>(
      `
        SELECT
          id,
          city_id,
          category,
          title,
          description,
          url,
          sort_order,
          is_active
        FROM public.move_city_services
        WHERE city_id = $1
          AND COALESCE(is_active, true) = true
        ORDER BY COALESCE(sort_order, 0), title
      `,
      [moveCase.to_city_id]
    );

    for (const service of cityServiceResult.rows) {
      await client.query(
        `
          INSERT INTO public.move_case_tasks (
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
            action_status
          )
          VALUES (
            $1, NULL, $2, $3,
            $4, $5,
            $4, NULL, NULL,
            $5, NULL, NULL,
            'open', $6, $7, $8, $9, true, true,
            'web',
            jsonb_build_object(
              'url', $8,
              'label', jsonb_build_object(
                'de', $9,
                'fr', $9,
                'en', $9
              )
            ),
            NULL,
            NULL,
            'pending'
          )
        `,
        [
          moveCase.id,
          service.id,
          service.category,
          service.title,
          service.description,
          addDays(moveCase.move_date, -7),
          service.sort_order ?? 0,
          service.url ?? null,
          'Service öffnen',
        ]
      );
    }

    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}