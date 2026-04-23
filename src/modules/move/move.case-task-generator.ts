import { PoolClient } from 'pg';
import { pool } from '../../db/pool';

type LanguageCode = 'de' | 'fr' | 'en';

type SyncContextRow = {
  case_id: string;
  user_id: string;
  language: string | null;
  to_city_id: string;
  to_canton_id: string | null;
  move_date: string;
  has_car: boolean | null;
  has_children: boolean | null;
  has_dog: boolean | null;
  children_count: number | null;
  marital_status: string | null;
  health_insurance_name: string | null;
  employer_name: string | null;
};

type MoveTaskTemplateRow = {
  id: string;
  category: string;
  service_slug: string | null;
  sort_order: number | null;
  is_active: boolean | null;
  requires_car: boolean | null;
  requires_children: boolean | null;
  requires_dog: boolean | null;
  trigger_before_move_days: number | null;
  trigger_after_move_days: number | null;
  header_de: string | null;
  header_fr: string | null;
  header_en: string | null;
  title_de: string | null;
  title_fr: string | null;
  title_en: string | null;
  description_de: string | null;
  description_fr: string | null;
  description_en: string | null;
  action_type: string | null;
  action_payload: Record<string, unknown> | null;
  secondary_action_type: string | null;
  secondary_action_payload: Record<string, unknown> | null;
};

type TemplateLinkRow = {
  task_template_id: string;
  city_id: string | null;
  canton_id: string | null;
  language_code: string | null;
  label_de: string | null;
  label_fr: string | null;
  label_en: string | null;
  url: string | null;
  sort_order: number | null;
  is_active: boolean | null;
};

type CityServiceRow = {
  id: string;
  city_id: string;
  service_slug: string;
  title_de: string | null;
  title_fr: string | null;
  title_en: string | null;
  description_de: string | null;
  description_fr: string | null;
  description_en: string | null;
  office_email: string | null;
  website_url: string | null;
  is_active: boolean | null;
};

type ExistingTaskRow = {
  id: string;
  template_id: string | null;
  status: 'open' | 'done' | 'skipped';
};

type GenericRow = Record<string, unknown>;

type MoveCaseAnswerContextRow = {
  question_key: string | null;
  answer_json: unknown;
  value_json: unknown;
  answer_text: string | null;
  value_text: string | null;
};

type MoveCaseContext = {
  caseId: string;
  userId: string;
  language: LanguageCode;
  toCityId: string;
  toCantonId: string | null;
  moveDate: string;
  hasCar: boolean;
  hasChildren: boolean;
  hasDog: boolean;
  childrenCount: number | null;
  maritalStatus: string | null;
  healthInsuranceName: string | null;
  employerName: string | null;
  values: Record<string, unknown>;
};

type VisibilityRule = {
  templateId: string;
  fieldKey: string;
  operator: '=' | 'in';
  value: unknown;
  active: boolean;
};

type TemplateCapability = {
  templateId: string;
  capabilityKey: string;
  payload: Record<string, unknown>;
  active: boolean;
  sortOrder: number;
};

type ResolvedTaskContext = {
  cityServiceId: string | null;
  externalUrl: string | null;
  linkLabel: string | null;
  isCitySpecific: boolean;
  service: CityServiceRow | null;
};

type TaskAction = {
  actionType: string;
  actionPayload: Record<string, unknown>;
};

type TaskActionMapping = {
  primary: TaskAction | null;
  secondary: TaskAction | null;
};

const SUPPORTED_ACTION_TYPES = new Set([
  'web',
  'email',
  'copy_text',
  'whatsapp',
  'pdf',
]);

type GeneratedTaskRow = {
  templateId: string;
  category: string;
  cityServiceId: string | null;
  headerDe: string | null;
  headerFr: string | null;
  headerEn: string | null;
  titleDe: string | null;
  titleFr: string | null;
  titleEn: string | null;
  descriptionDe: string | null;
  descriptionFr: string | null;
  descriptionEn: string | null;
  title: string;
  description: string | null;
  dueDate: string;
  sortOrder: number;
  externalUrl: string | null;
  linkLabel: string | null;
  isRequired: boolean;
  isCitySpecific: boolean;
  actionType: string | null;
  actionPayload: Record<string, unknown> | null;
  secondaryActionType: string | null;
  secondaryActionPayload: Record<string, unknown> | null;
};

const schemaWarningCache = new Set<string>();

function warnSchemaFallback(key: string, message: string, error?: unknown) {
  if (schemaWarningCache.has(key)) {
    return;
  }

  schemaWarningCache.add(key);

  const details =
    error instanceof Error && error.message
      ? ` ${error.message}`
      : '';

  console.warn(`[move-generator] ${message}${details}`);
}

const RENDERABLE_CAPABILITIES = new Set([
  'prepare_email',
  'prepare_whatsapp',
  'open_link',
  'copy_text',
]);

function normalizeLanguageCode(value: string | null | undefined): LanguageCode {
  const trimmed = String(value ?? 'de').trim().toLowerCase();
  if (trimmed.startsWith('fr')) return 'fr';
  if (trimmed.startsWith('en')) return 'en';
  return 'de';
}

function addDays(dateString: string, days: number) {
  const date = new Date(dateString);
  date.setDate(date.getDate() + days);
  return date.toISOString().slice(0, 10);
}

function asString(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  const normalized = String(value).trim();
  return normalized.length ? normalized : null;
}

function asBoolean(value: unknown): boolean | null {
  if (typeof value === 'boolean') return value;
  if (typeof value === 'number') return value !== 0;
  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase();
    if (['true', '1', 'yes', 'ja', 'oui'].includes(normalized)) return true;
    if (['false', '0', 'no', 'nein', 'non'].includes(normalized)) return false;
  }
  return null;
}

function asNumber(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string' && value.trim().length) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function parseJsonLike(value: unknown): unknown {
  if (typeof value !== 'string') return value;
  const trimmed = value.trim();
  if (!trimmed) return null;
  if (
    (trimmed.startsWith('{') && trimmed.endsWith('}')) ||
    (trimmed.startsWith('[') && trimmed.endsWith(']'))
  ) {
    try {
      return JSON.parse(trimmed);
    } catch {
      return value;
    }
  }
  return value;
}

function toRecord(value: unknown): Record<string, unknown> {
  const parsed = parseJsonLike(value);
  if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
    return parsed as Record<string, unknown>;
  }
  return {};
}

function toArray(value: unknown): unknown[] {
  const parsed = parseJsonLike(value);
  if (Array.isArray(parsed)) return parsed;
  if (typeof parsed === 'string' && parsed.includes(',')) {
    return parsed
      .split(',')
      .map((item) => item.trim())
      .filter(Boolean);
  }
  if (parsed === null || parsed === undefined || parsed === '') return [];
  return [parsed];
}

async function loadTableColumns(
  client: { query: PoolClient['query'] },
  schemaName: string,
  tableName: string
) {
  const result = await client.query<{ column_name: string }>(
    `
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = $1
      AND table_name = $2
    `,
    [schemaName, tableName]
  );

  const columns = new Set(result.rows.map((row) => row.column_name));

  if (columns.size === 0) {
    warnSchemaFallback(
      `table-columns:${schemaName}.${tableName}`,
      `No columns discovered for ${schemaName}.${tableName}; move task generator will degrade defensively.`
    );
  }

  return columns;
}

function pickFirst(row: GenericRow, keys: string[]): unknown {
  for (const key of keys) {
    if (Object.prototype.hasOwnProperty.call(row, key) && row[key] !== undefined) {
      return row[key];
    }
  }
  return undefined;
}

function localizedText(
  language: LanguageCode,
  values: { de?: string | null; fr?: string | null; en?: string | null },
  fallback: string | null = null
): string | null {
  if (language === 'fr') return values.fr || values.de || values.en || fallback;
  if (language === 'en') return values.en || values.de || values.fr || fallback;
  return values.de || values.fr || values.en || fallback;
}

function buildLocalizedPayloadValue(
  payload: Record<string, unknown>,
  key: string,
  fallback?: { de?: string | null; fr?: string | null; en?: string | null }
) {
  const nested = payload[key];
  if (nested && typeof nested === 'object' && !Array.isArray(nested)) {
    const record = nested as Record<string, unknown>;
    return {
      de: asString(record.de) ?? fallback?.de ?? null,
      fr: asString(record.fr) ?? fallback?.fr ?? null,
      en: asString(record.en) ?? fallback?.en ?? null,
    };
  }

  const scalar = asString(nested);
  return {
    de: scalar ?? asString(payload[`${key}_de`]) ?? fallback?.de ?? null,
    fr: asString(payload[`${key}_fr`]) ?? scalar ?? fallback?.fr ?? null,
    en: asString(payload[`${key}_en`]) ?? scalar ?? fallback?.en ?? null,
  };
}

function normalizeRule(row: GenericRow): VisibilityRule | null {
  const templateId = asString(
    pickFirst(row, ['task_template_id', 'template_id', 'move_task_template_id'])
  );
  const fieldKey = asString(
    pickFirst(row, ['field_key', 'context_key', 'field', 'attribute', 'attribute_key'])
  );
  const operatorText =
    asString(pickFirst(row, ['operator', 'comparison_operator']))?.toLowerCase() ?? '=';
  const ruleType =
    asString(pickFirst(row, ['rule_type', 'type', 'kind', 'scope']))?.toLowerCase() ??
    'visibility';

  if (!templateId || !fieldKey || ruleType !== 'visibility') return null;

  return {
    templateId,
    fieldKey,
    operator: operatorText === 'in' ? 'in' : '=',
    value: parseJsonLike(
      pickFirst(row, ['value_json', 'values_json', 'expected_value', 'value', 'values'])
    ),
    active: asBoolean(pickFirst(row, ['is_active', 'active'])) ?? true,
  };
}

function normalizeCapability(row: GenericRow): TemplateCapability | null {
  const templateId = asString(
    pickFirst(row, ['task_template_id', 'template_id', 'move_task_template_id'])
  );
  const capabilityKey = asString(
    pickFirst(row, [
      'capability_key',
      'capability_slug',
      'capability',
      'type',
      'kind',
      'slug',
    ])
  );

  if (!templateId || !capabilityKey) return null;

  return {
    templateId,
    capabilityKey,
    payload: toRecord(
      pickFirst(row, ['payload_json', 'config_json', 'payload', 'config', 'metadata'])
    ),
    active: asBoolean(pickFirst(row, ['is_active', 'active'])) ?? true,
    sortOrder: asNumber(pickFirst(row, ['sort_order', 'position', 'priority'])) ?? 0,
  };
}

export function evaluateVisibilityRules(
  rules: VisibilityRule[],
  context: MoveCaseContext
): boolean {
  const activeRules = rules.filter((rule) => rule.active);
  if (activeRules.length === 0) return true;

  return activeRules.every((rule) => {
    const currentValue = context.values[rule.fieldKey];

    if (rule.operator === 'in') {
      const allowedValues = toArray(rule.value).map((item) => String(item).toLowerCase());
      return allowedValues.includes(String(currentValue ?? '').toLowerCase());
    }

    const currentBool = asBoolean(currentValue);
    const ruleBool = asBoolean(rule.value);
    if (currentBool !== null && ruleBool !== null) {
      return currentBool === ruleBool;
    }

    const currentNumber = asNumber(currentValue);
    const ruleNumber = asNumber(rule.value);
    if (currentNumber !== null && ruleNumber !== null) {
      return currentNumber === ruleNumber;
    }

    return String(currentValue ?? '').toLowerCase() === String(rule.value ?? '').toLowerCase();
  });
}

export function calculateDueDate(template: MoveTaskTemplateRow, moveDate: string): string {
  if (template.trigger_before_move_days !== null) {
    return addDays(moveDate, -template.trigger_before_move_days);
  }

  if (template.trigger_after_move_days !== null) {
    return addDays(moveDate, template.trigger_after_move_days);
  }

  return moveDate;
}

function pickBestLink(
  links: TemplateLinkRow[],
  language: LanguageCode,
  cityId: string,
  cantonId: string | null
): TemplateLinkRow | null {
  const candidates = links
    .filter((link) => link.is_active !== false)
    .filter((link) => {
      const linkLanguage = asString(link.language_code)?.toLowerCase();
      return !linkLanguage || linkLanguage === language;
    })
    .sort((a, b) => {
      const score = (row: TemplateLinkRow) => {
        if (row.city_id && row.city_id === cityId) return 1;
        if (!row.city_id && row.canton_id && cantonId && row.canton_id === cantonId) return 2;
        return 3;
      };

      const scoreDiff = score(a) - score(b);
      if (scoreDiff !== 0) return scoreDiff;
      return (a.sort_order ?? 0) - (b.sort_order ?? 0);
    });

  return candidates[0] ?? null;
}

export function resolveTaskContext(
  template: MoveTaskTemplateRow,
  context: MoveCaseContext,
  servicesBySlug: Map<string, CityServiceRow>,
  linksByTemplateId: Map<string, TemplateLinkRow[]>
): ResolvedTaskContext {
  const service =
    template.service_slug && servicesBySlug.has(template.service_slug)
      ? servicesBySlug.get(template.service_slug) ?? null
      : null;

  if (service) {
    return {
      cityServiceId: service.id,
      externalUrl: service.website_url ?? null,
      linkLabel:
        localizedText(context.language, {
          de: service.title_de,
          fr: service.title_fr,
          en: service.title_en,
        }) ?? null,
      isCitySpecific: true,
      service,
    };
  }

  const selectedLink = pickBestLink(
    linksByTemplateId.get(template.id) ?? [],
    context.language,
    context.toCityId,
    context.toCantonId
  );

  return {
    cityServiceId: null,
    externalUrl: selectedLink?.url ?? null,
    linkLabel:
      localizedText(context.language, {
        de: selectedLink?.label_de ?? null,
        fr: selectedLink?.label_fr ?? null,
        en: selectedLink?.label_en ?? null,
      }) ?? null,
    isCitySpecific: Boolean(selectedLink?.city_id || selectedLink?.canton_id),
    service: null,
  };
}

function createFallbackAction(
  externalUrl: string | null,
  linkLabel: string | null
): TaskActionMapping {
  if (!externalUrl) {
    return { primary: null, secondary: null };
  }

  return {
    primary: {
      actionType: 'web',
      actionPayload: {
        url: externalUrl,
        label: { de: linkLabel, fr: linkLabel, en: linkLabel },
      },
    },
    secondary: null,
  };
}

export function mapCapabilitiesToActions(
  capabilities: TemplateCapability[],
  resolvedContext: ResolvedTaskContext
): TaskActionMapping {
  const actions: TaskAction[] = [];

  for (const capability of capabilities
    .filter((item) => item.active)
    .sort((a, b) => a.sortOrder - b.sortOrder)) {
    if (!RENDERABLE_CAPABILITIES.has(capability.capabilityKey)) {
      continue;
    }

    const label = buildLocalizedPayloadValue(capability.payload, 'label', {
      de: resolvedContext.linkLabel,
      fr: resolvedContext.linkLabel,
      en: resolvedContext.linkLabel,
    });

    if (capability.capabilityKey === 'prepare_email') {
      const recipientSource = asString(capability.payload.recipient_source);
      const to = asString(capability.payload.to) ?? resolvedContext.service?.office_email ?? null;
      if (!to && recipientSource !== 'user_input') continue;
      actions.push({
        actionType: 'email',
        actionPayload: {
          to,
          recipient_source: recipientSource,
          label,
          subject: buildLocalizedPayloadValue(capability.payload, 'subject'),
          body: buildLocalizedPayloadValue(capability.payload, 'body'),
        },
      });
      continue;
    }

    if (capability.capabilityKey === 'prepare_whatsapp') {
      actions.push({
        actionType: 'whatsapp',
        actionPayload: {
          label,
          message: buildLocalizedPayloadValue(capability.payload, 'message'),
        },
      });
      continue;
    }

    if (capability.capabilityKey === 'copy_text') {
      actions.push({
        actionType: 'copy_text',
        actionPayload: {
          label,
          text: buildLocalizedPayloadValue(capability.payload, 'text'),
        },
      });
      continue;
    }

    if (capability.capabilityKey === 'open_link') {
      const url = asString(capability.payload.url) ?? resolvedContext.externalUrl;
      if (!url) continue;
      actions.push({
        actionType: 'web',
        actionPayload: {
          url,
          label,
        },
      });
    }
  }

  if (actions.length === 0) {
    return createFallbackAction(resolvedContext.externalUrl, resolvedContext.linkLabel);
  }

  return {
    primary: actions[0] ?? null,
    secondary: actions[1] ?? null,
  };
}

function mapLegacyTemplateActions(template: MoveTaskTemplateRow): TaskActionMapping {
  const primaryActionType = asString(template.action_type);
  const secondaryActionType = asString(template.secondary_action_type);

  const primary =
    primaryActionType && SUPPORTED_ACTION_TYPES.has(primaryActionType)
      ? {
          actionType: primaryActionType,
          actionPayload: template.action_payload ?? {},
        }
      : null;

  const secondary =
    secondaryActionType && SUPPORTED_ACTION_TYPES.has(secondaryActionType)
      ? {
          actionType: secondaryActionType,
          actionPayload: template.secondary_action_payload ?? {},
        }
      : null;

  return {
    primary,
    secondary,
  };
}

function buildTaskRow(
  template: MoveTaskTemplateRow,
  context: MoveCaseContext,
  resolvedContext: ResolvedTaskContext,
  actions: TaskActionMapping
): GeneratedTaskRow {
  return {
    templateId: template.id,
    category: template.category,
    cityServiceId: resolvedContext.cityServiceId,
    headerDe: template.header_de,
    headerFr: template.header_fr,
    headerEn: template.header_en,
    titleDe: template.title_de,
    titleFr: template.title_fr,
    titleEn: template.title_en,
    descriptionDe: template.description_de,
    descriptionFr: template.description_fr,
    descriptionEn: template.description_en,
    title:
      localizedText(
        context.language,
        {
          de: template.title_de,
          fr: template.title_fr,
          en: template.title_en,
        },
        'Aufgabe'
      ) ?? 'Aufgabe',
    description:
      localizedText(context.language, {
        de: template.description_de,
        fr: template.description_fr,
        en: template.description_en,
      }) ?? null,
    dueDate: calculateDueDate(template, context.moveDate),
    sortOrder: template.sort_order ?? 0,
    externalUrl: resolvedContext.externalUrl,
    linkLabel: resolvedContext.linkLabel,
    isRequired: true,
    isCitySpecific: resolvedContext.isCitySpecific,
    actionType: actions.primary?.actionType ?? null,
    actionPayload: actions.primary?.actionPayload ?? null,
    secondaryActionType: actions.secondary?.actionType ?? null,
    secondaryActionPayload: actions.secondary?.actionPayload ?? null,
  };
}

export async function upsertMoveCaseTask(
  client: PoolClient,
  caseId: string,
  task: GeneratedTaskRow,
  existingTasksByTemplateId: Map<string, ExistingTaskRow>,
  doneTemplateIds: Set<string>,
  taskTableColumns: Set<string>
) {
  if (doneTemplateIds.has(task.templateId)) {
    return;
  }

  const existingTask = existingTasksByTemplateId.get(task.templateId);

  if (existingTask && existingTask.status !== 'done') {
    const updateAssignments: string[] = [];
    const updateValues: unknown[] = [];

    const addUpdate = (column: string, value: unknown) => {
      if (!taskTableColumns.has(column)) return;
      updateAssignments.push(`${column} = $${updateValues.length + 1}`);
      updateValues.push(value);
    };

    if (taskTableColumns.has('status')) {
      updateAssignments.push(`status = 'open'`);
    }

    addUpdate('city_service_id', task.cityServiceId);
    addUpdate('category', task.category);
    addUpdate(
      'header',
      localizedText('de', { de: task.headerDe, fr: task.headerFr, en: task.headerEn })
    );
    addUpdate('header_de', task.headerDe);
    addUpdate('header_fr', task.headerFr);
    addUpdate('header_en', task.headerEn);
    addUpdate('title', task.title);
    addUpdate('title_de', task.titleDe);
    addUpdate('title_fr', task.titleFr);
    addUpdate('title_en', task.titleEn);
    addUpdate('description', task.description);
    addUpdate('description_de', task.descriptionDe);
    addUpdate('description_fr', task.descriptionFr);
    addUpdate('description_en', task.descriptionEn);
    addUpdate('due_date', task.dueDate);
    addUpdate('sort_order', task.sortOrder);
    addUpdate('external_url', task.externalUrl);
    addUpdate('link_label', task.linkLabel);
    addUpdate('is_required', task.isRequired);
    addUpdate('is_city_specific', task.isCitySpecific);
    addUpdate('action_type', task.actionType);
    addUpdate('action_payload', task.actionPayload);
    addUpdate('secondary_action_type', task.secondaryActionType);
    addUpdate('secondary_action_payload', task.secondaryActionPayload);

    if (taskTableColumns.has('action_status')) {
      updateAssignments.push(`action_status = COALESCE(action_status, 'pending')`);
    }

    if (taskTableColumns.has('updated_at')) {
      updateAssignments.push('updated_at = NOW()');
    }

    if (updateAssignments.length === 0) {
      warnSchemaFallback(
        'move_case_tasks_update_columns',
        'move_case_tasks has no updatable known columns; existing task row is left unchanged.'
      );
      return;
    }

    await client.query(
      `
      UPDATE move_case_tasks
      SET
        ${updateAssignments.join(',\n        ')}
      WHERE id = $${updateValues.length + 1}
      `,
      [...updateValues, existingTask.id]
    );
    return;
  }

  const insertColumns: string[] = [];
  const insertValues: unknown[] = [];

  const addInsert = (column: string, value: unknown) => {
    if (!taskTableColumns.has(column)) return;
    insertColumns.push(column);
    insertValues.push(value);
  };

  addInsert('case_id', caseId);
  addInsert('template_id', task.templateId);
  addInsert('city_service_id', task.cityServiceId);
  addInsert('category', task.category);
  addInsert(
    'header',
    localizedText('de', { de: task.headerDe, fr: task.headerFr, en: task.headerEn })
  );
  addInsert('header_de', task.headerDe);
  addInsert('header_fr', task.headerFr);
  addInsert('header_en', task.headerEn);
  addInsert('title', task.title);
  addInsert('title_de', task.titleDe);
  addInsert('title_fr', task.titleFr);
  addInsert('title_en', task.titleEn);
  addInsert('description', task.description);
  addInsert('description_de', task.descriptionDe);
  addInsert('description_fr', task.descriptionFr);
  addInsert('description_en', task.descriptionEn);
  addInsert('status', 'open');
  addInsert('due_date', task.dueDate);
  addInsert('sort_order', task.sortOrder);
  addInsert('external_url', task.externalUrl);
  addInsert('link_label', task.linkLabel);
  addInsert('is_required', task.isRequired);
  addInsert('is_city_specific', task.isCitySpecific);
  addInsert('action_type', task.actionType);
  addInsert('action_payload', task.actionPayload);
  addInsert('secondary_action_type', task.secondaryActionType);
  addInsert('secondary_action_payload', task.secondaryActionPayload);
  addInsert('action_status', 'pending');

  if (!taskTableColumns.has('case_id') || !taskTableColumns.has('template_id')) {
    throw new Error(
      'move_case_tasks schema is incompatible: required columns case_id/template_id are missing'
    );
  }

  const placeholders = insertColumns.map((_, index) => `$${index + 1}`);

  await client.query(
    `
    INSERT INTO move_case_tasks (
      ${insertColumns.join(', ')}
    )
    VALUES (
      ${placeholders.join(', ')}
    )
    `,
    insertValues
  );
}

async function loadMoveCaseContext(client: PoolClient, caseId: string): Promise<MoveCaseContext> {
  const result = await client.query<SyncContextRow>(
    `
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
    `,
    [caseId]
  );

  if (!result.rows[0]) {
    throw new Error('Move case not found');
  }

  const row = result.rows[0];
  const context: MoveCaseContext = {
    caseId: row.case_id,
    userId: row.user_id,
    language: normalizeLanguageCode(row.language),
    toCityId: row.to_city_id,
    toCantonId: row.to_canton_id,
    moveDate: row.move_date,
    hasCar: Boolean(row.has_car),
    hasChildren: Boolean(row.has_children),
    hasDog: Boolean(row.has_dog),
    childrenCount: row.children_count ?? null,
    maritalStatus: row.marital_status ?? null,
    healthInsuranceName: row.health_insurance_name ?? null,
    employerName: row.employer_name ?? null,
    values: {},
  };

  context.values = {
    has_car: context.hasCar,
    hasCar: context.hasCar,
    has_children: context.hasChildren,
    hasChildren: context.hasChildren,
    has_dog: context.hasDog,
    hasDog: context.hasDog,
    children_count: context.childrenCount,
    childrenCount: context.childrenCount,
    marital_status: context.maritalStatus,
    maritalStatus: context.maritalStatus,
    health_insurance_name: context.healthInsuranceName,
    healthInsuranceName: context.healthInsuranceName,
    employer_name: context.employerName,
    employerName: context.employerName,
    to_city_id: context.toCityId,
    toCityId: context.toCityId,
    to_canton_id: context.toCantonId,
    toCantonId: context.toCantonId,
    move_date: context.moveDate,
    moveDate: context.moveDate,
  };

  await client.query(
    `
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
    `,
    [
      context.hasCar,
      context.hasChildren,
      context.hasDog,
      context.childrenCount,
      context.maritalStatus,
      context.healthInsuranceName,
      context.employerName,
      caseId,
    ]
  );

  return context;
}

async function loadTemplates(client: PoolClient) {
  const templateColumns = await loadTableColumns(client, 'public', 'move_task_templates');

  const serviceSlugSelect = templateColumns.has('service_slug')
    ? 'service_slug'
    : templateColumns.has('slug')
      ? 'slug AS service_slug'
      : 'NULL::text AS service_slug';

  const headerDeSelect = templateColumns.has('header_de')
    ? "NULLIF(header_de, '') AS header_de"
    : 'NULL::text AS header_de';
  const headerFrSelect = templateColumns.has('header_fr')
    ? "NULLIF(header_fr, '') AS header_fr"
    : 'NULL::text AS header_fr';
  const headerEnSelect = templateColumns.has('header_en')
    ? "NULLIF(header_en, '') AS header_en"
    : 'NULL::text AS header_en';
  const actionTypeSelect = templateColumns.has('action_type')
    ? 'action_type'
    : 'NULL::text AS action_type';
  const actionPayloadSelect = templateColumns.has('action_payload')
    ? 'action_payload'
    : 'NULL::jsonb AS action_payload';
  const secondaryActionTypeSelect = templateColumns.has('secondary_action_type')
    ? 'secondary_action_type'
    : 'NULL::text AS secondary_action_type';
  const secondaryActionPayloadSelect = templateColumns.has('secondary_action_payload')
    ? 'secondary_action_payload'
    : 'NULL::jsonb AS secondary_action_payload';

  if (
    !templateColumns.has('service_slug') ||
    !templateColumns.has('header_de') ||
    !templateColumns.has('header_fr') ||
    !templateColumns.has('header_en')
  ) {
    warnSchemaFallback(
      'move_task_templates_legacy_columns',
      'move_task_templates is missing newer columns; generator falls back to legacy template fields.'
    );
  }

  const result = await client.query<MoveTaskTemplateRow>(
    `
    SELECT
      id,
      category,
      ${serviceSlugSelect},
      sort_order,
      is_active,
      requires_car,
      requires_children,
      requires_dog,
      trigger_before_move_days,
      trigger_after_move_days,
      ${headerDeSelect},
      ${headerFrSelect},
      ${headerEnSelect},
      NULLIF(title_de, '') AS title_de,
      NULLIF(title_fr, '') AS title_fr,
      NULLIF(title_en, '') AS title_en,
      NULLIF(description_de, '') AS description_de,
      NULLIF(description_fr, '') AS description_fr,
      NULLIF(description_en, '') AS description_en,
      ${actionTypeSelect},
      ${actionPayloadSelect},
      ${secondaryActionTypeSelect},
      ${secondaryActionPayloadSelect}
    FROM move_task_templates
    WHERE COALESCE(is_active, true) = true
    ORDER BY COALESCE(sort_order, 0), created_at, id
    `
  );

  return result.rows;
}

async function loadRulesByTemplateId(client: PoolClient) {
  let rows: GenericRow[] = [];

  try {
    const result = await client.query<GenericRow>('SELECT * FROM move_task_template_rules');
    rows = result.rows;
  } catch (error) {
    const code =
      typeof error === 'object' && error !== null && 'code' in error
        ? String((error as { code?: unknown }).code ?? '')
        : '';

    if (code !== '42P01' && code !== '42703') {
      throw error;
    }

    warnSchemaFallback(
      'move_task_template_rules',
      'move_task_template_rules is unavailable; visibility rules are skipped.',
      error
    );
  }

  const grouped = new Map<string, VisibilityRule[]>();

  for (const row of rows) {
    const normalized = normalizeRule(row);
    if (!normalized) continue;
    const list = grouped.get(normalized.templateId) ?? [];
    list.push(normalized);
    grouped.set(normalized.templateId, list);
  }

  return grouped;
}

async function loadCapabilitiesByTemplateId(client: PoolClient) {
  let rows: GenericRow[] = [];

  try {
    const result = await client.query<GenericRow>('SELECT * FROM move_task_template_capabilities');
    rows = result.rows;
  } catch (error) {
    const code =
      typeof error === 'object' && error !== null && 'code' in error
        ? String((error as { code?: unknown }).code ?? '')
        : '';

    if (code !== '42P01' && code !== '42703') {
      throw error;
    }

    warnSchemaFallback(
      'move_task_template_capabilities',
      'move_task_template_capabilities is unavailable; capability mapping falls back to classic links.',
      error
    );
  }

  const grouped = new Map<string, TemplateCapability[]>();

  for (const row of rows) {
    const normalized = normalizeCapability(row);
    if (!normalized) continue;
    const list = grouped.get(normalized.templateId) ?? [];
    list.push(normalized);
    grouped.set(normalized.templateId, list);
  }

  return grouped;
}

async function loadLinksByTemplateId(client: PoolClient, templateIds: string[]) {
  if (templateIds.length === 0) return new Map<string, TemplateLinkRow[]>();

  let rows: TemplateLinkRow[] = [];

  try {
    const result = await client.query<TemplateLinkRow>(
      `
      SELECT
        task_template_id,
        city_id,
        canton_id,
        language_code,
        label_de,
        label_fr,
        label_en,
        url,
        sort_order,
        is_active
      FROM move_task_template_links
      WHERE task_template_id::text = ANY($1::text[])
      `,
      [templateIds]
    );

    rows = result.rows;
  } catch (error) {
    const code =
      typeof error === 'object' && error !== null && 'code' in error
        ? String((error as { code?: unknown }).code ?? '')
        : '';

    if (code !== '42P01' && code !== '42703') {
      throw error;
    }

    warnSchemaFallback(
      'move_task_template_links_legacy_table',
      'move_task_template_links is unavailable; generator falls back to legacy move_task_links.',
      error
    );

    const fallbackResult = await client.query<TemplateLinkRow>(
      `
      SELECT
        task_template_id,
        city_id,
        canton_id,
        language_code,
        label_de,
        label_fr,
        label_en,
        url,
        sort_order,
        is_active
      FROM move_task_links
      WHERE task_template_id::text = ANY($1::text[])
      `,
      [templateIds]
    );

    rows = fallbackResult.rows;
  }

  const grouped = new Map<string, TemplateLinkRow[]>();
  for (const row of rows) {
    const key = String(row.task_template_id);
    const list = grouped.get(key) ?? [];
    list.push(row);
    grouped.set(key, list);
  }
  return grouped;
}

async function loadAnswerContextValues(client: PoolClient, caseId: string) {
  try {
    const answerColumns = await loadTableColumns(client, 'public', 'move_case_task_answers');

    const valueJsonSelect = answerColumns.has('value_json')
      ? 'a.value_json'
      : 'NULL::jsonb AS value_json';
    const answerTextSelect = answerColumns.has('answer_text')
      ? 'a.answer_text'
      : 'NULL::text AS answer_text';
    const valueTextSelect = answerColumns.has('value_text')
      ? 'a.value_text'
      : 'NULL::text AS value_text';

    const result = await client.query<MoveCaseAnswerContextRow>(
      `
      SELECT
        a.question_key,
        a.answer_json,
        ${valueJsonSelect},
        ${answerTextSelect},
        ${valueTextSelect}
      FROM move_case_task_answers a
      INNER JOIN move_case_tasks t
        ON t.id = a.case_task_id
      WHERE t.case_id = $1
      `,
      [caseId]
    );

    const values: Record<string, unknown> = {};

    for (const row of result.rows) {
      const questionKey = asString(row.question_key);
      if (!questionKey) continue;

      const answerValue =
        parseJsonLike(row.answer_json) ??
        parseJsonLike(row.value_json) ??
        row.answer_text ??
        row.value_text ??
        null;

      values[questionKey] = answerValue;
    }

    return values;
  } catch (error) {
    const code =
      typeof error === 'object' && error !== null && 'code' in error
        ? String((error as { code?: unknown }).code ?? '')
        : '';

    if (code === '42P01' || code === '42703') {
      return {};
    }

    throw error;
  }
}

async function loadCityServicesBySlug(
  client: PoolClient,
  cityId: string,
  serviceSlugs: string[]
) {
  if (serviceSlugs.length === 0) return new Map<string, CityServiceRow>();

  const result = await client.query<CityServiceRow>(
    `
    SELECT
      id,
      city_id,
      service_slug,
      title_de,
      title_fr,
      title_en,
      description_de,
      description_fr,
      description_en,
      office_email,
      website_url,
      is_active
    FROM move_city_services
    WHERE city_id = $1
      AND service_slug = ANY($2::text[])
      AND COALESCE(is_active, true) = true
    `,
    [cityId, serviceSlugs]
  );

  return new Map(result.rows.map((row) => [String(row.service_slug), row]));
}

async function loadExistingTasks(client: PoolClient, caseId: string) {
  const result = await client.query<ExistingTaskRow>(
    `
    SELECT id, template_id, status
    FROM move_case_tasks
    WHERE case_id = $1
      AND template_id IS NOT NULL
    `,
    [caseId]
  );

  const existingTasksByTemplateId = new Map<string, ExistingTaskRow>();
  const doneTemplateIds = new Set<string>();

  for (const row of result.rows) {
    if (!row.template_id) continue;
    const templateId = String(row.template_id);
    if (row.status === 'done') {
      doneTemplateIds.add(templateId);
    } else {
      existingTasksByTemplateId.set(templateId, row);
    }
  }

  return {
    existingTasks: result.rows,
    existingTasksByTemplateId,
    doneTemplateIds,
  };
}

export async function generateMoveCaseTasks(caseId: string) {
  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    let context: MoveCaseContext;
    try {
      console.info('[MOVE_TASK_GENERATOR_STEP]', {
        caseId,
        step: 'loadMoveCaseContext',
      });
      context = await loadMoveCaseContext(client, caseId);
    } catch (error) {
      console.error('[MOVE_TASK_GENERATOR_STEP_ERROR]', {
        caseId,
        step: 'loadMoveCaseContext',
        error: error instanceof Error ? error.message : String(error),
        code:
          typeof error === 'object' && error !== null && 'code' in error
            ? String((error as { code?: unknown }).code ?? '')
            : '',
        stack: error instanceof Error ? error.stack ?? null : null,
      });
      throw error;
    }

    let answerContextValues: Record<string, unknown>;
    try {
      console.info('[MOVE_TASK_GENERATOR_STEP]', {
        caseId,
        step: 'loadAnswerContextValues',
      });
      answerContextValues = await loadAnswerContextValues(client, caseId);
    } catch (error) {
      console.error('[MOVE_TASK_GENERATOR_STEP_ERROR]', {
        caseId,
        step: 'loadAnswerContextValues',
        error: error instanceof Error ? error.message : String(error),
        code:
          typeof error === 'object' && error !== null && 'code' in error
            ? String((error as { code?: unknown }).code ?? '')
            : '',
        stack: error instanceof Error ? error.stack ?? null : null,
      });
      throw error;
    }

    context.values = {
      ...context.values,
      ...answerContextValues,
    };

    let templates: MoveTaskTemplateRow[];
    try {
      console.info('[MOVE_TASK_GENERATOR_STEP]', {
        caseId,
        step: 'loadTemplates',
      });
      templates = await loadTemplates(client);
    } catch (error) {
      console.error('[MOVE_TASK_GENERATOR_STEP_ERROR]', {
        caseId,
        step: 'loadTemplates',
        error: error instanceof Error ? error.message : String(error),
        code:
          typeof error === 'object' && error !== null && 'code' in error
            ? String((error as { code?: unknown }).code ?? '')
            : '',
        stack: error instanceof Error ? error.stack ?? null : null,
      });
      throw error;
    }

    const templateIds = templates.map((template) => String(template.id));

    let rulesByTemplateId: Awaited<ReturnType<typeof loadRulesByTemplateId>>;
    try {
      console.info('[MOVE_TASK_GENERATOR_STEP]', {
        caseId,
        step: 'loadRulesByTemplateId',
      });
      rulesByTemplateId = await loadRulesByTemplateId(client);
    } catch (error) {
      console.error('[MOVE_TASK_GENERATOR_STEP_ERROR]', {
        caseId,
        step: 'loadRulesByTemplateId',
        error: error instanceof Error ? error.message : String(error),
        code:
          typeof error === 'object' && error !== null && 'code' in error
            ? String((error as { code?: unknown }).code ?? '')
            : '',
        stack: error instanceof Error ? error.stack ?? null : null,
      });
      throw error;
    }

    let capabilitiesByTemplateId: Awaited<ReturnType<typeof loadCapabilitiesByTemplateId>>;
    try {
      console.info('[MOVE_TASK_GENERATOR_STEP]', {
        caseId,
        step: 'loadCapabilitiesByTemplateId',
      });
      capabilitiesByTemplateId = await loadCapabilitiesByTemplateId(client);
    } catch (error) {
      console.error('[MOVE_TASK_GENERATOR_STEP_ERROR]', {
        caseId,
        step: 'loadCapabilitiesByTemplateId',
        error: error instanceof Error ? error.message : String(error),
        code:
          typeof error === 'object' && error !== null && 'code' in error
            ? String((error as { code?: unknown }).code ?? '')
            : '',
        stack: error instanceof Error ? error.stack ?? null : null,
      });
      throw error;
    }

    let linksByTemplateId: Awaited<ReturnType<typeof loadLinksByTemplateId>>;
    try {
      console.info('[MOVE_TASK_GENERATOR_STEP]', {
        caseId,
        step: 'loadLinksByTemplateId',
      });
      linksByTemplateId = await loadLinksByTemplateId(client, templateIds);
    } catch (error) {
      console.error('[MOVE_TASK_GENERATOR_STEP_ERROR]', {
        caseId,
        step: 'loadLinksByTemplateId',
        error: error instanceof Error ? error.message : String(error),
        code:
          typeof error === 'object' && error !== null && 'code' in error
            ? String((error as { code?: unknown }).code ?? '')
            : '',
        stack: error instanceof Error ? error.stack ?? null : null,
      });
      throw error;
    }

    let existingTaskData: Awaited<ReturnType<typeof loadExistingTasks>>;
    try {
      console.info('[MOVE_TASK_GENERATOR_STEP]', {
        caseId,
        step: 'loadExistingTasks',
      });
      existingTaskData = await loadExistingTasks(client, caseId);
    } catch (error) {
      console.error('[MOVE_TASK_GENERATOR_STEP_ERROR]', {
        caseId,
        step: 'loadExistingTasks',
        error: error instanceof Error ? error.message : String(error),
        code:
          typeof error === 'object' && error !== null && 'code' in error
            ? String((error as { code?: unknown }).code ?? '')
            : '',
        stack: error instanceof Error ? error.stack ?? null : null,
      });
      throw error;
    }

    const taskTableColumns = await loadTableColumns(client, 'public', 'move_case_tasks');

    const serviceSlugs = templates
      .map((template) => asString(template.service_slug))
      .filter((slug): slug is string => Boolean(slug));

    let servicesBySlug: Awaited<ReturnType<typeof loadCityServicesBySlug>>;
    try {
      console.info('[MOVE_TASK_GENERATOR_STEP]', {
        caseId,
        step: 'loadCityServicesBySlug',
      });
      servicesBySlug = await loadCityServicesBySlug(client, context.toCityId, serviceSlugs);
    } catch (error) {
      console.error('[MOVE_TASK_GENERATOR_STEP_ERROR]', {
        caseId,
        step: 'loadCityServicesBySlug',
        error: error instanceof Error ? error.message : String(error),
        code:
          typeof error === 'object' && error !== null && 'code' in error
            ? String((error as { code?: unknown }).code ?? '')
            : '',
        stack: error instanceof Error ? error.stack ?? null : null,
      });
      throw error;
    }

    const relevantTemplateIds = new Set<string>();

    console.info('[MOVE_TASK_GENERATOR_STEP]', {
      caseId,
      step: 'upsertMoveCaseTaskLoop',
      templateCount: templates.length,
    });

    for (const template of templates) {
      try {
        if (template.requires_car === true && !context.hasCar) continue;
        if (template.requires_children === true && !context.hasChildren) continue;
        if (template.requires_dog === true && !context.hasDog) continue;

        const rules = rulesByTemplateId.get(String(template.id)) ?? [];
        if (!evaluateVisibilityRules(rules, context)) {
          continue;
        }

        relevantTemplateIds.add(String(template.id));

        const resolvedContext = resolveTaskContext(
          template,
          context,
          servicesBySlug,
          linksByTemplateId
        );

        const capabilityActions = mapCapabilitiesToActions(
          capabilitiesByTemplateId.get(String(template.id)) ?? [],
          resolvedContext
        );

        const legacyActions =
          capabilityActions.primary || capabilityActions.secondary
            ? capabilityActions
            : mapLegacyTemplateActions(template);

        const taskRow = buildTaskRow(
          template,
          context,
          resolvedContext,
          legacyActions.primary || legacyActions.secondary
            ? legacyActions
            : createFallbackAction(resolvedContext.externalUrl, resolvedContext.linkLabel)
        );

        await upsertMoveCaseTask(
          client,
          caseId,
          taskRow,
          existingTaskData.existingTasksByTemplateId,
          existingTaskData.doneTemplateIds,
          taskTableColumns
        );
      } catch (error) {
        console.error('[MOVE_TASK_GENERATOR_STEP_ERROR]', {
          caseId,
          step: 'upsertMoveCaseTaskLoop',
          templateId: template.id,
          error: error instanceof Error ? error.message : String(error),
          code:
            typeof error === 'object' && error !== null && 'code' in error
              ? String((error as { code?: unknown }).code ?? '')
              : '',
          stack: error instanceof Error ? error.stack ?? null : null,
        });
        throw error;
      }
    }

    for (const task of existingTaskData.existingTasks) {
      if (
        task.status !== 'done' &&
        task.template_id &&
        !relevantTemplateIds.has(String(task.template_id))
      ) {
        await client.query(
          `
          UPDATE move_case_tasks
          SET
            status = 'skipped',
            updated_at = NOW()
          WHERE id = $1
            AND case_id = $2
            AND status <> 'done'
          `,
          [task.id, caseId]
        );
      }
    }

    await client.query('COMMIT');
  } catch (error) {
    console.error('[MOVE_TASK_GENERATOR_FATAL]', {
      caseId,
      error: error instanceof Error ? error.message : String(error),
      code:
        typeof error === 'object' && error !== null && 'code' in error
          ? String((error as { code?: unknown }).code ?? '')
          : '',
      stack: error instanceof Error ? error.stack ?? null : null,
    });
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}