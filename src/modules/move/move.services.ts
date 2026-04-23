import fs from 'fs';
import path from 'path';
import PDFDocument = require('pdfkit');

import { pool } from '../../db/pool';
import { refreshMoveCaseTasks } from './move.task-sync';

type MoveCaseTaskRow = {
  id: string;
  case_id: string;
  template_id: string | null;
  city_service_id: string | null;
  category: string;

  title: string;
  description: string | null;

  title_de: string | null;
  title_fr: string | null;
  title_en: string | null;

  description_de: string | null;
  description_fr: string | null;
  description_en: string | null;

  status: string;
  due_date: string | null;
  sort_order: number;
  external_url: string | null;
  link_label: string | null;
  is_required: boolean;
  is_city_specific: boolean;

  action_type: string | null;
  action_payload: any | null;
  secondary_action_type: string | null;
  secondary_action_payload: any | null;
  action_status: string | null;

  completed_at: string | null;
  created_at: string;
  updated_at: string;
};

type IdRow = {
  id: string;
};

type GenericRow = Record<string, unknown>;

type MoveCaseTaskDetailRow = MoveCaseTaskRow & {
  header: string | null;
  header_de: string | null;
  header_fr: string | null;
  header_en: string | null;
};

type CityServiceDetailRow = {
  id: string;
  office_name: string | null;
  office_address: string | null;
  office_email: string | null;
  office_phone: string | null;
  website_url: string | null;
  online_available: boolean | null;
  appointment_required: boolean | null;
};

type LocalizedValue = {
  de: string | null;
  fr: string | null;
  en: string | null;
};

type MoveTaskQuestionDto = {
  id: string | null;
  key: string | null;
  type: string | null;
  label: string | null;
  description: string | null;
  placeholder: string | null;
  help_text: string | null;
  options: Array<{
    value: string;
    label: string | null;
    label_de: string | null;
    label_fr: string | null;
    label_en: string | null;
  }>;
  is_required: boolean;
  sort_order: number;
  label_i18n: LocalizedValue;
  description_i18n: LocalizedValue;
  placeholder_i18n: LocalizedValue;
  help_text_i18n: LocalizedValue;
};

type MoveTaskAnswerDto = {
  id: string | null;
  question_id: string | null;
  question_key: string | null;
  value: unknown;
  value_text: string | null;
  created_at: string | null;
  updated_at: string | null;
};

type MoveTaskOutputDto = {
  id: string | null;
  output_key: string | null;
  type: string | null;
  title: string | null;
  content: unknown;
  created_at: string | null;
  updated_at: string | null;
};

type MoveTaskEntityDto = {
  id: string | null;
  entity_type: string | null;
  entity_key: string | null;
  title: string | null;
  data: Record<string, unknown>;
  created_at: string | null;
  updated_at: string | null;
};

type MoveTaskActionDto = {
  type: string;
  payload: Record<string, unknown> | null;
};

type MoveTaskFormCompletionSummaryDto = {
  required: number;
  answered: number;
  is_complete: boolean;
};

type MoveCaseTaskDetailDto = ReturnType<typeof mapMoveCaseTask> & {
  header: string | null;
  header_de: string | null;
  header_fr: string | null;
  header_en: string | null;
  city_service: {
    office_name: string | null;
    office_address: string | null;
    office_email: string | null;
    office_phone: string | null;
    website_url: string | null;
    online_available: boolean;
    appointment_required: boolean;
  } | null;
  questions: MoveTaskQuestionDto[];
  answers: MoveTaskAnswerDto[];
  outputs: MoveTaskOutputDto[];
  entities: MoveTaskEntityDto[];
  primary_action: MoveTaskActionDto | null;
  secondary_action: MoveTaskActionDto | null;
  form_completion: MoveTaskFormCompletionSummaryDto;
};

type SaveMoveTaskAnswerInput = {
  question_key: string;
  answer: unknown;
};

type SupportedMoveTaskActionType =
  | 'web'
  | 'email'
  | 'copy_text'
  | 'whatsapp'
  | 'pdf';

type ExecuteMoveTaskActionResult = {
  action: {
    type: SupportedMoveTaskActionType;
    status: 'prepared';
    data: Record<string, unknown>;
    output: MoveTaskOutputDto | null;
  };
  task: MoveCaseTaskDetailDto;
  nextTaskId: string | null;
};

type MoveCaseContextRow = {
  id: string;
  move_date: string | null;
  from_street: string | null;
  from_house_number: string | null;
  from_zip: string | null;
  to_street: string | null;
  to_house_number: string | null;
  to_zip: string | null;
  from_city_name: string | null;
  to_city_name: string | null;
};

const schemaWarningCache = new Set<string>();

function warnSchemaFallback(key: string, message: string, error?: unknown) {
  if (schemaWarningCache.has(key)) {
    return;
  }

  schemaWarningCache.add(key);

  const details =
    error instanceof Error && error.message ? ` ${error.message}` : '';

  console.warn(`[move] ${message}${details}`);
}

function mapMoveCaseTask(row: MoveCaseTaskRow) {
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
  if (parsed === null || parsed === undefined || parsed === '') return [];
  return [parsed];
}

function pickFirst(row: GenericRow, keys: string[]): unknown {
  for (const key of keys) {
    if (Object.prototype.hasOwnProperty.call(row, key) && row[key] !== undefined) {
      return row[key];
    }
  }

  return undefined;
}

function localizedValue(row: GenericRow, baseKey: string): LocalizedValue {
  return {
    de: asString(row[`${baseKey}_de`]),
    fr: asString(row[`${baseKey}_fr`]),
    en: asString(row[`${baseKey}_en`]),
  };
}

function localizedScalar(
  values: LocalizedValue,
  fallback?: string | null
): string | null {
  return values.de ?? values.fr ?? values.en ?? fallback ?? null;
}

function localizedPayloadScalar(
  value: unknown,
  fallback?: string | null
): string | null {
  if (value && typeof value === 'object' && !Array.isArray(value)) {
    const record = value as Record<string, unknown>;
    return (
      asString(record.de) ??
      asString(record.fr) ??
      asString(record.en) ??
      fallback ??
      null
    );
  }

  return asString(value) ?? fallback ?? null;
}

function normalizeActionType(value: string): string {
  return String(value ?? '').trim().toLowerCase();
}

function normalizeLanguageCode(value: unknown): 'de' | 'fr' | 'en' {
  const normalized = String(value ?? '')
    .trim()
    .toLowerCase();

  if (normalized === 'fr') return 'fr';
  if (normalized === 'en') return 'en';
  return 'de';
}

function mapQuestionOption(option: unknown) {
  if (option && typeof option === 'object' && !Array.isArray(option)) {
    const record = option as Record<string, unknown>;
    const labelI18n =
      record.label && typeof record.label === 'object'
        ? {
            de: asString((record.label as Record<string, unknown>).de),
            fr: asString((record.label as Record<string, unknown>).fr),
            en: asString((record.label as Record<string, unknown>).en),
          }
        : {
            de: asString(record.label_de) ?? asString(record.label),
            fr: asString(record.label_fr) ?? asString(record.label),
            en: asString(record.label_en) ?? asString(record.label),
          };

    return {
      value: asString(record.value) ?? '',
      label: localizedScalar(labelI18n),
      label_de: labelI18n.de,
      label_fr: labelI18n.fr,
      label_en: labelI18n.en,
    };
  }

  return {
    value: asString(option) ?? '',
    label: asString(option),
    label_de: asString(option),
    label_fr: asString(option),
    label_en: asString(option),
  };
}

function mapQuestionRow(row: GenericRow): MoveTaskQuestionDto {
  const labelI18n = localizedValue(row, 'label');
  const descriptionI18n = localizedValue(row, 'description');
  const placeholderI18n = localizedValue(row, 'placeholder');
  const helpTextI18n = localizedValue(row, 'help_text');
  const rawOptions = pickFirst(row, ['options_json', 'options', 'choices_json', 'choices']);

  return {
    id: asString(pickFirst(row, ['id'])),
    key: asString(pickFirst(row, ['question_key', 'field_key', 'key', 'slug'])),
    type: asString(pickFirst(row, ['question_type', 'type', 'input_type'])),
    label: localizedScalar(labelI18n, asString(pickFirst(row, ['label']))),
    description: localizedScalar(
      descriptionI18n,
      asString(pickFirst(row, ['description']))
    ),
    placeholder: localizedScalar(
      placeholderI18n,
      asString(pickFirst(row, ['placeholder']))
    ),
    help_text: localizedScalar(
      helpTextI18n,
      asString(pickFirst(row, ['help_text', 'helper_text', 'hint']))
    ),
    options: toArray(rawOptions)
      .map(mapQuestionOption)
      .filter((option) => option.value.length > 0),
    is_required: asBoolean(pickFirst(row, ['is_required', 'required'])) ?? false,
    sort_order: asNumber(pickFirst(row, ['sort_order', 'position'])) ?? 0,
    label_i18n: labelI18n,
    description_i18n: descriptionI18n,
    placeholder_i18n: placeholderI18n,
    help_text_i18n: helpTextI18n,
  };
}

function mapAnswerRow(row: GenericRow): MoveTaskAnswerDto {
  const value = parseJsonLike(
    pickFirst(row, ['answer_json', 'value_json', 'answer', 'value'])
  );

  return {
    id: asString(pickFirst(row, ['id'])),
    question_id: asString(pickFirst(row, ['question_id', 'template_question_id'])),
    question_key: asString(pickFirst(row, ['question_key', 'field_key', 'key'])),
    value,
    value_text:
      asString(pickFirst(row, ['value_text', 'answer_text'])) ??
      (typeof value === 'string' ? value : null),
    created_at: asString(pickFirst(row, ['created_at'])),
    updated_at: asString(pickFirst(row, ['updated_at'])),
  };
}

function mapOutputRow(row: GenericRow): MoveTaskOutputDto {
  const payload = parseJsonLike(
    pickFirst(row, ['payload_json', 'content_json', 'payload', 'content'])
  );
  const payloadRecord =
    payload && typeof payload === 'object' && !Array.isArray(payload)
      ? (payload as Record<string, unknown>)
      : {};

  const titleI18n =
    payloadRecord.title && typeof payloadRecord.title === 'object'
      ? {
          de: asString((payloadRecord.title as Record<string, unknown>).de),
          fr: asString((payloadRecord.title as Record<string, unknown>).fr),
          en: asString((payloadRecord.title as Record<string, unknown>).en),
        }
      : localizedValue(row, 'title');

  return {
    id: asString(pickFirst(row, ['id'])),
    output_key:
      asString(pickFirst(row, ['output_key', 'key', 'slug'])) ??
      asString(payloadRecord.outputKey) ??
      asString(pickFirst(row, ['output_type', 'type', 'kind'])),
    type: asString(pickFirst(row, ['output_type', 'type', 'kind'])),
    title:
      localizedScalar(titleI18n, asString(pickFirst(row, ['title']))) ?? null,
    content: payload ?? null,
    created_at:
      asString(pickFirst(row, ['generated_at'])) ??
      asString(pickFirst(row, ['created_at'])),
    updated_at: asString(pickFirst(row, ['updated_at'])),
  };
}

function mapEntityRow(row: GenericRow): MoveTaskEntityDto {
  const titleI18n = localizedValue(row, 'title');

  return {
    id: asString(pickFirst(row, ['id'])),
    entity_type: asString(pickFirst(row, ['entity_type', 'type'])),
    entity_key: asString(pickFirst(row, ['entity_key', 'key', 'slug'])),
    title: localizedScalar(titleI18n, asString(pickFirst(row, ['title']))),
    data: toRecord(pickFirst(row, ['data_json', 'entity_json', 'data', 'payload'])),
    created_at: asString(pickFirst(row, ['created_at'])),
    updated_at: asString(pickFirst(row, ['updated_at'])),
  };
}

function hasAnswerValue(value: unknown): boolean {
  if (value === null || value === undefined) return false;
  if (typeof value === 'string') return value.trim().length > 0;
  if (Array.isArray(value)) return value.length > 0;
  if (typeof value === 'object') {
    return Object.keys(value as Record<string, unknown>).length > 0;
  }
  return true;
}

function buildFormCompletionSummary(
  questions: MoveTaskQuestionDto[],
  answers: MoveTaskAnswerDto[]
): MoveTaskFormCompletionSummaryDto {
  const requiredQuestions = questions.filter((question) => question.is_required);

  if (requiredQuestions.length === 0) {
    return {
      required: 0,
      answered: 0,
      is_complete: true,
    };
  }

  const answeredQuestionIds = new Set<string>();
  const answeredQuestionKeys = new Set<string>();

  for (const answer of answers) {
    if (!hasAnswerValue(answer.value) && !answer.value_text) {
      continue;
    }

    if (answer.question_id) {
      answeredQuestionIds.add(answer.question_id);
    }

    if (answer.question_key) {
      answeredQuestionKeys.add(answer.question_key);
    }
  }

  const answered = requiredQuestions.filter((question) => {
    if (question.id && answeredQuestionIds.has(question.id)) return true;
    if (question.key && answeredQuestionKeys.has(question.key)) return true;
    return false;
  }).length;

  return {
    required: requiredQuestions.length,
    answered,
    is_complete: answered >= requiredQuestions.length,
  };
}

async function loadOptionalTableRows(
  tableName: string,
  whereColumn: string,
  whereValue: string
) {
  try {
    const result = await pool.query<GenericRow>(
      `SELECT * FROM ${tableName} WHERE ${whereColumn} = $1 ORDER BY created_at ASC, id ASC`,
      [whereValue]
    );

    return result.rows;
  } catch (error) {
    const code =
      typeof error === 'object' && error !== null && 'code' in error
        ? String((error as { code?: unknown }).code ?? '')
        : '';

    if (code === '42P01' || code === '42703') {
      warnSchemaFallback(
        `optional-table:${tableName}:${whereColumn}`,
        `Optional move schema resource unavailable for ${tableName}.${whereColumn}; continuing with empty result.`,
        error
      );
      return [];
    }

    throw error;
  }
}

async function loadTableColumns(
  client: { query: typeof pool.query },
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
      `No columns discovered for ${schemaName}.${tableName}; move feature will degrade defensively.`
    );
  }

  return columns;
}

function selectColumn(
  columns: Set<string>,
  alias: string,
  columnName: string,
  fallbackType: 'text' | 'boolean' | 'jsonb' | 'integer' | 'timestamp'
) {
  if (columns.has(columnName)) {
    return `${alias}.${columnName}`;
  }

  return `NULL::${fallbackType} AS ${columnName}`;
}

function toJsonValue(value: unknown): unknown {
  if (value === undefined) return null;
  return value;
}

function isSupportedMoveTaskActionType(
  value: string
): value is SupportedMoveTaskActionType {
  const normalized = normalizeActionType(value);

  return (
    normalized === 'web' ||
    normalized === 'email' ||
    normalized === 'copy_text' ||
    normalized === 'whatsapp' ||
    normalized === 'pdf'
  );
}

async function findNextRelevantMoveTaskId(
  caseId: string,
  currentTaskId?: string | null
): Promise<string | null> {
  const result = await pool.query<IdRow>(
    `
      SELECT id
      FROM public.move_case_tasks
      WHERE case_id = $1
        AND status = 'open'
        AND ($2::text IS NULL OR id::text <> $2::text)
      ORDER BY sort_order ASC, created_at ASC
      LIMIT 1
    `,
    [caseId, currentTaskId ?? null]
  );

  return result.rows[0]?.id ?? null;
}

async function loadOwnedMoveTaskRow(
  client: { query: typeof pool.query },
  caseId: string,
  taskId: string,
  userId: number | string
) {
  const [taskColumns, cityServiceColumns] = await Promise.all([
    loadTableColumns(client, 'public', 'move_case_tasks'),
    loadTableColumns(client, 'public', 'move_city_services'),
  ]);

  const cityServiceJoin =
    taskColumns.has('city_service_id') && cityServiceColumns.has('id')
      ? 'LEFT JOIN public.move_city_services cs ON cs.id = t.city_service_id'
      : '';

  const taskResult = await client.query<MoveCaseTaskDetailRow & CityServiceDetailRow>(
    `
      SELECT
        t.id,
        t.case_id,
        ${selectColumn(taskColumns, 't', 'template_id', 'text')},
        ${selectColumn(taskColumns, 't', 'city_service_id', 'text')},
        ${selectColumn(taskColumns, 't', 'category', 'text')},

        ${selectColumn(taskColumns, 't', 'header', 'text')},
        ${selectColumn(taskColumns, 't', 'header_de', 'text')},
        ${selectColumn(taskColumns, 't', 'header_fr', 'text')},
        ${selectColumn(taskColumns, 't', 'header_en', 'text')},

        ${selectColumn(taskColumns, 't', 'title', 'text')},
        ${selectColumn(taskColumns, 't', 'description', 'text')},

        ${selectColumn(taskColumns, 't', 'title_de', 'text')},
        ${selectColumn(taskColumns, 't', 'title_fr', 'text')},
        ${selectColumn(taskColumns, 't', 'title_en', 'text')},

        ${selectColumn(taskColumns, 't', 'description_de', 'text')},
        ${selectColumn(taskColumns, 't', 'description_fr', 'text')},
        ${selectColumn(taskColumns, 't', 'description_en', 'text')},

        ${selectColumn(taskColumns, 't', 'status', 'text')},
        ${selectColumn(taskColumns, 't', 'due_date', 'text')},
        ${selectColumn(taskColumns, 't', 'sort_order', 'integer')},
        ${selectColumn(taskColumns, 't', 'external_url', 'text')},
        ${selectColumn(taskColumns, 't', 'link_label', 'text')},
        ${selectColumn(taskColumns, 't', 'is_required', 'boolean')},
        ${selectColumn(taskColumns, 't', 'is_city_specific', 'boolean')},

        ${selectColumn(taskColumns, 't', 'action_type', 'text')},
        ${selectColumn(taskColumns, 't', 'action_payload', 'jsonb')},
        ${selectColumn(taskColumns, 't', 'secondary_action_type', 'text')},
        ${selectColumn(taskColumns, 't', 'secondary_action_payload', 'jsonb')},
        ${selectColumn(taskColumns, 't', 'action_status', 'text')},

        ${selectColumn(taskColumns, 't', 'completed_at', 'text')},
        ${selectColumn(taskColumns, 't', 'created_at', 'text')},
        ${selectColumn(taskColumns, 't', 'updated_at', 'text')},

        ${selectColumn(cityServiceColumns, 'cs', 'office_name', 'text')},
        ${selectColumn(cityServiceColumns, 'cs', 'office_address', 'text')},
        ${selectColumn(cityServiceColumns, 'cs', 'office_email', 'text')},
        ${selectColumn(cityServiceColumns, 'cs', 'office_phone', 'text')},
        ${selectColumn(cityServiceColumns, 'cs', 'website_url', 'text')},
        ${selectColumn(cityServiceColumns, 'cs', 'online_available', 'boolean')},
        ${selectColumn(cityServiceColumns, 'cs', 'appointment_required', 'boolean')}
      FROM public.move_case_tasks t
      INNER JOIN public.move_cases mc
        ON mc.id = t.case_id
      ${cityServiceJoin}
      WHERE t.id = $1
        AND t.case_id = $2
        AND mc.user_id = $3
      LIMIT 1
    `,
    [taskId, caseId, userId]
  );

  if (!taskResult.rowCount) {
    throw new Error('Move task not found');
  }

  return taskResult.rows[0];
}

async function loadMoveCaseContext(caseId: string): Promise<MoveCaseContextRow | null> {
  const result = await pool.query<MoveCaseContextRow>(
    `
      SELECT
        mc.id,
        mc.move_date,
        mc.from_street,
        mc.from_house_number,
        mc.from_zip,
        mc.to_street,
        mc.to_house_number,
        mc.to_zip,
        fc.name AS from_city_name,
        tc.name AS to_city_name
      FROM public.move_cases mc
      LEFT JOIN public.move_cities fc ON fc.id = mc.from_city_id
      LEFT JOIN public.move_cities tc ON tc.id = mc.to_city_id
      WHERE mc.id = $1
      LIMIT 1
    `,
    [caseId]
  );

  return result.rows[0] ?? null;
}

function formatAddress(parts: Array<string | null | undefined>): string {
  return parts
    .map((part) => asString(part))
    .filter((part): part is string => Boolean(part))
    .join(' ');
}

function buildOldAddress(moveCase: MoveCaseContextRow | null): string {
  if (!moveCase) return '';
  const line1 = formatAddress([moveCase.from_street, moveCase.from_house_number]);
  const line2 = formatAddress([moveCase.from_zip, moveCase.from_city_name]);
  return [line1, line2].filter(Boolean).join(', ');
}

function buildNewAddress(moveCase: MoveCaseContextRow | null): string {
  if (!moveCase) return '';
  const line1 = formatAddress([moveCase.to_street, moveCase.to_house_number]);
  const line2 = formatAddress([moveCase.to_zip, moveCase.to_city_name]);
  return [line1, line2].filter(Boolean).join(', ');
}

function replaceTemplateVariables(
  template: string | null,
  values: Record<string, string | null | undefined>
): string {
  if (!template) return '';

  return template.replace(/{{\s*([a-zA-Z0-9_]+)\s*}}/g, (_, key: string) => {
    return values[key] ?? '';
  });
}

async function loadOutputTemplate(
  taskTemplateId: string | null,
  outputType: 'email' | 'copy_text' | 'pdf',
  languageCode: 'de' | 'fr' | 'en' = 'de'
): Promise<GenericRow | null> {
  if (!taskTemplateId) return null;

  try {
    const result = await pool.query<GenericRow>(
      `
        SELECT *
        FROM public.move_task_template_output_templates
        WHERE task_template_id = $1
          AND output_type = $2
          AND language_code = $3
          AND is_active = true
        ORDER BY created_at DESC
        LIMIT 1
      `,
      [taskTemplateId, outputType, languageCode]
    );

    return result.rows[0] ?? null;
  } catch (error) {
    warnSchemaFallback(
      `output-template:${taskTemplateId}:${outputType}:${languageCode}`,
      'move_task_template_output_templates unavailable; action falls back to payload/default.',
      error
    );
    return null;
  }
}

function ensureGeneratedDir() {
  const dir = path.join(process.cwd(), 'public', 'generated');
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

function splitNonEmptyLines(value: string | null | undefined): string[] {
  if (!value) return [];
  return value
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean);
}

function formatDateByLanguage(
  value: string | null | undefined,
  languageCode: 'de' | 'fr' | 'en'
): string {
  if (!value) return '';

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return String(value);
  }

  const locale =
    languageCode === 'fr'
      ? 'fr-CH'
      : languageCode === 'en'
        ? 'en-CH'
        : 'de-CH';

  return new Intl.DateTimeFormat(locale, {
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
  }).format(date);
}

function normalizeAddressLines(value: string | null | undefined): string[] {
  if (!value) return [];

  return value
    .replace(/,\s*/g, '\n')
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean);
}

function drawLines(
  doc: PDFKit.PDFDocument,
  lines: string[],
  options?: { fontSize?: number; bold?: boolean; lineGap?: number }
) {
  if (!lines.length) return;

  doc.font(options?.bold ? 'Helvetica-Bold' : 'Helvetica');
  doc.fontSize(options?.fontSize ?? 11);

  for (const line of lines) {
    doc.text(line, {
      align: 'left',
      lineGap: options?.lineGap ?? 1,
    });
  }
}

function drawParagraphs(
  doc: PDFKit.PDFDocument,
  paragraphs: string[],
  options?: { fontSize?: number }
) {
  doc.font('Helvetica');
  doc.fontSize(options?.fontSize ?? 11);

  for (const paragraph of paragraphs) {
    const trimmed = paragraph.trim();

    if (!trimmed) {
      doc.moveDown();
      continue;
    }

    doc.text(trimmed, {
      align: 'left',
      lineGap: 2,
    });

    doc.moveDown();
  }
}

async function generatePdfFile(params: {
  fileName: string;
  subject: string;
  senderLines?: string[];
  recipientLines?: string[];
  placeDateLine?: string;
  greeting?: string;
  bodyParagraphs: string[];
  closingLines?: string[];
}): Promise<{ filePath: string; fileUrl: string }> {
  const generatedDir = ensureGeneratedDir();
  const filePath = path.join(generatedDir, params.fileName);

  await new Promise<void>((resolve, reject) => {
    const doc = new PDFDocument({ size: 'A4', margin: 50 });
    const stream = fs.createWriteStream(filePath);

    doc.pipe(stream);

    const senderLines = (params.senderLines ?? []).filter(Boolean);
    const recipientLines = (params.recipientLines ?? []).filter(Boolean);
    const closingLines = (params.closingLines ?? []).filter(Boolean);

    // Absender oben links
    if (senderLines.length) {
      drawLines(doc, senderLines, { fontSize: 10, lineGap: 1 });
      doc.moveDown(2);
    }

    // Empfängerblock
    if (recipientLines.length) {
      drawLines(doc, recipientLines, { fontSize: 11, lineGap: 1 });
      doc.moveDown(2);
    }

    // Ort / Datum
    if (params.placeDateLine?.trim()) {
      doc.font('Helvetica');
      doc.fontSize(11).text(params.placeDateLine.trim(), { align: 'left' });
      doc.moveDown(2);
    }

    // Betreff
    doc.font('Helvetica-Bold');
    doc.fontSize(13).text(params.subject, { align: 'left' });
    doc.moveDown(1.5);

    // Anrede
    if (params.greeting?.trim()) {
      doc.font('Helvetica');
      doc.fontSize(11).text(params.greeting.trim(), { align: 'left' });
      doc.moveDown();
    }

    // Fliesstext
    drawParagraphs(doc, params.bodyParagraphs, { fontSize: 11 });

    // Schlussblock
    if (closingLines.length) {
      doc.moveDown(0.5);
      drawLines(doc, closingLines, { fontSize: 11, lineGap: 1 });
    }

    doc.end();

    stream.on('finish', () => resolve());
    stream.on('error', (error) => reject(error));
  });

  return {
    filePath,
    fileUrl: `https://api.halloch.ch/generated/${params.fileName}`,
  };
}

async function saveMoveTaskOutput(
  client: { query: typeof pool.query },
  caseTaskId: string,
  outputKey: string,
  outputType: string,
  title: string | null,
  content: Record<string, unknown>
): Promise<void> {
  const payloadJson = toJsonValue({
    outputKey,
    title,
    ...content,
  });

  const fileUrl =
    typeof content.file_url === 'string'
      ? content.file_url
      : typeof content.url === 'string'
        ? content.url
        : null;

  const updateResult = await client.query(
    `
      UPDATE public.move_case_task_outputs
      SET
        status = 'prepared',
        payload_json = $1,
        file_url = $2,
        generated_at = NOW(),
        updated_at = NOW()
      WHERE case_task_id = $3
        AND output_type = $4
    `,
    [payloadJson, fileUrl, caseTaskId, outputType]
  );

  if (updateResult.rowCount && updateResult.rowCount > 0) {
    return;
  }

  await client.query(
    `
      INSERT INTO public.move_case_task_outputs (
        case_task_id,
        output_type,
        status,
        payload_json,
        file_url,
        generated_at,
        created_at,
        updated_at
      )
      VALUES ($1, $2, 'prepared', $3, $4, NOW(), NOW(), NOW())
    `,
    [caseTaskId, outputType, payloadJson, fileUrl]
  );
}

function resolveTaskAction(
  task: MoveCaseTaskDetailDto,
  actionType: SupportedMoveTaskActionType
): MoveTaskActionDto {
  const candidates = [task.primary_action, task.secondary_action].filter(
    (action): action is MoveTaskActionDto => Boolean(action)
  );

  const matched = candidates.find(
    (action) => normalizeActionType(action.type) === actionType
  );

  if (!matched) {
    throw new Error('Move task action not available');
  }

  return matched;
}

function buildActionContext(
  task: MoveCaseTaskDetailDto,
  moveCase: MoveCaseContextRow | null,
  languageCode: 'de' | 'fr' | 'en' = 'de'
) {
  const answersByKey = new Map<string, unknown>();

  for (const answer of task.answers) {
    if (!answer.question_key) continue;
    answersByKey.set(answer.question_key, answer.value ?? answer.value_text ?? null);
  }

  const oldAddress = buildOldAddress(moveCase);
  const newAddress = buildNewAddress(moveCase);
  const moveDate = asString(moveCase?.move_date) ?? '';

  return {
    task,
    moveCase,
    languageCode,
    answersByKey,
    replacements: {
      recipient_name: asString(answersByKey.get('recipient_name')) ?? '',
      recipient_email: asString(answersByKey.get('recipient_email')) ?? '',
      recipient_address: asString(answersByKey.get('recipient_address')) ?? '',
      sender_name: asString(answersByKey.get('sender_name')) ?? '',
      sender_address: asString(answersByKey.get('sender_address')) ?? '',
      full_name: asString(answersByKey.get('full_name')) ?? '',
      delivery_method: asString(answersByKey.get('delivery_method')) ?? '',
      termination_date: asString(answersByKey.get('termination_date')) ?? moveDate,
      contract_reference: asString(answersByKey.get('contract_reference')) ?? '',
      rental_object_address:
        asString(answersByKey.get('rental_object_address')) ?? oldAddress,
      old_address: asString(answersByKey.get('old_address')) ?? oldAddress,
      new_address: asString(answersByKey.get('new_address')) ?? newAddress,
      move_date: asString(answersByKey.get('move_date')) ?? moveDate,
      document_date: asString(answersByKey.get('document_date')) ?? '',
      newAddress: asString(answersByKey.get('new_address')) ?? newAddress,
      moveDate: asString(answersByKey.get('move_date')) ?? moveDate,
    },
  };
}

function buildWebActionData(
  task: MoveCaseTaskDetailDto,
  action: MoveTaskActionDto
) {
  const payload = action.payload ?? {};
  const url =
    asString(payload.url) ??
    task.externalUrl ??
    task.city_service?.website_url ??
    null;

  if (!url) {
    throw new Error('Move task action payload invalid');
  }

  return {
    url,
    label: localizedPayloadScalar(payload.label, task.linkLabel ?? task.title),
  };
}

async function buildEmailActionData(
  context: ReturnType<typeof buildActionContext>,
  action: MoveTaskActionDto
) {
  const payload = action.payload ?? {};
  const templateRow = await loadOutputTemplate(
    context.task.templateId,
    'email',
    context.languageCode
  );

  const subjectTemplate =
    asString(templateRow?.subject_template) ??
    localizedPayloadScalar(payload.subject) ??
    `${context.task.title}`;

  const bodyTemplate =
    asString(templateRow?.body_template) ??
    localizedPayloadScalar(payload.body) ??
    [
      context.task.description,
      context.task.city_service?.office_name,
      context.task.city_service?.office_address,
    ]
      .filter(Boolean)
      .join('\n\n');

  const to =
    asString(payload.to) ??
    asString(context.answersByKey.get('recipient_email')) ??
    context.task.city_service?.office_email ??
    null;

  const subject = replaceTemplateVariables(subjectTemplate, context.replacements);
  const body = replaceTemplateVariables(bodyTemplate, context.replacements);

  const defaultLabel =
    context.languageCode === 'fr'
      ? 'Préparer l’e-mail'
      : context.languageCode === 'en'
        ? 'Prepare email'
        : 'E-Mail vorbereiten';

  return {
    to,
    subject,
    body,
    label: localizedPayloadScalar(payload.label, context.task.linkLabel ?? defaultLabel),
  };
}

async function buildCopyTextActionData(
  context: ReturnType<typeof buildActionContext>,
  action: MoveTaskActionDto
) {
  const payload = action.payload ?? {};
  const templateRow = await loadOutputTemplate(
    context.task.templateId,
    'copy_text',
    context.languageCode
  );

  const textTemplate =
    asString(templateRow?.body_template) ??
    localizedPayloadScalar(payload.text) ??
    localizedPayloadScalar(payload.body) ??
    asString(context.answersByKey.get('copy_text')) ??
    context.task.description ??
    context.task.title;

  const text = replaceTemplateVariables(textTemplate, context.replacements);

  const defaultLabel =
    context.languageCode === 'fr'
      ? 'Copier le texte'
      : context.languageCode === 'en'
        ? 'Copy text'
        : 'Text kopieren';

  return {
    text,
    label: localizedPayloadScalar(payload.label, context.task.linkLabel ?? defaultLabel),
  };
}

function buildWhatsappActionData(
  context: ReturnType<typeof buildActionContext>,
  action: MoveTaskActionDto
) {
  const payload = action.payload ?? {};
  const textTemplate =
    localizedPayloadScalar(payload.message) ??
    localizedPayloadScalar(payload.body) ??
    context.task.description ??
    context.task.title;

  const text = replaceTemplateVariables(textTemplate, context.replacements);

  const defaultLabel =
    context.languageCode === 'fr'
      ? 'Préparer WhatsApp'
      : context.languageCode === 'en'
        ? 'Prepare WhatsApp'
        : 'WhatsApp vorbereiten';

  return {
    text,
    label: localizedPayloadScalar(payload.label, defaultLabel),
  };
}

async function buildPdfActionData(
  context: ReturnType<typeof buildActionContext>,
  action: MoveTaskActionDto,
  taskId: string
) {
  const payload = action.payload ?? {};
  const templateRow = await loadOutputTemplate(
    context.task.templateId,
    'pdf',
    context.languageCode
  );
  const fileTemplateJson = toRecord(templateRow?.file_template_json);

  const subject =
    asString(fileTemplateJson.title) ??
    localizedPayloadScalar((payload as Record<string, unknown>).title) ??
    context.task.title ??
    'Dokument';

  const bodyTemplate =
    asString(fileTemplateJson.body) ??
    localizedPayloadScalar((payload as Record<string, unknown>).body) ??
    context.task.description ??
    '';

  const body = replaceTemplateVariables(bodyTemplate, context.replacements);

  const greeting =
    context.languageCode === 'fr'
      ? 'Madame, Monsieur'
      : context.languageCode === 'en'
        ? 'Dear Sir or Madam,'
        : 'Sehr geehrte Damen und Herren';

  const closing =
    context.languageCode === 'fr'
      ? 'Salutations distinguées'
      : context.languageCode === 'en'
        ? 'Kind regards'
        : 'Freundliche Grüsse';

  const recipientName =
    asString(context.answersByKey.get('recipient_name')) ??
    context.task.city_service?.office_name ??
    '';

  const recipientAddress =
    asString(context.answersByKey.get('recipient_address')) ??
    context.task.city_service?.office_address ??
    '';

  const recipientLines = [
    recipientName,
    ...normalizeAddressLines(recipientAddress),
  ].filter(Boolean);

  const senderName =
    asString(context.answersByKey.get('sender_name')) ??
    asString(context.answersByKey.get('full_name')) ??
    '';

  const senderAddress =
    asString(context.answersByKey.get('sender_address')) ??
    context.replacements.old_address ??
    '';

  const senderLines = [
    senderName,
    ...normalizeAddressLines(senderAddress),
  ].filter(Boolean);

  const cityForDate =
    context.moveCase?.from_city_name ??
    context.moveCase?.to_city_name ??
    '';

  const documentDateRaw =
    asString(context.answersByKey.get('document_date')) ?? new Date().toISOString();

  const formattedDate = formatDateByLanguage(
    documentDateRaw,
    context.languageCode
  );

  const placeDateLine = [cityForDate, formattedDate].filter(Boolean).join(', ');

  const bodyParagraphs = body
    .split('\n\n')
    .map((paragraph) => paragraph.trim())
    .filter(Boolean);

  const closingLines = [closing, '', senderName].filter((line, index) => {
    if (line) return true;
    return index === 1 && Boolean(senderName);
  });

  const fileName = `task-${taskId}-${Date.now()}.pdf`;
  const pdf = await generatePdfFile({
    fileName,
    subject,
    senderLines,
    recipientLines,
    placeDateLine,
    greeting,
    bodyParagraphs,
    closingLines,
  });

  const defaultLabel =
    context.languageCode === 'fr'
      ? 'Télécharger le PDF'
      : context.languageCode === 'en'
        ? 'Download PDF'
        : 'PDF herunterladen';

  return {
    file_url: pdf.fileUrl,
    title: subject,
    body,
    label: localizedPayloadScalar(payload.label, defaultLabel),
  };
}

export async function getMoveCaseTasks(caseId: string, userId: number | string) {
  if (!caseId?.trim()) {
    throw new Error('Case id is required');
  }

  const ownershipResult = await pool.query(
    `
      SELECT id
      FROM public.move_cases
      WHERE id = $1
        AND user_id = $2
      LIMIT 1
    `,
    [caseId, userId]
  );

  if (!ownershipResult.rowCount) {
    throw new Error('Move case not found');
  }

  const taskColumns = await loadTableColumns(pool, 'public', 'move_case_tasks');

  const result = await pool.query<MoveCaseTaskRow>(
    `
      SELECT
        id,
        case_id,
        ${selectColumn(taskColumns, 'move_case_tasks', 'template_id', 'text')},
        ${selectColumn(taskColumns, 'move_case_tasks', 'city_service_id', 'text')},
        ${selectColumn(taskColumns, 'move_case_tasks', 'category', 'text')},

        ${selectColumn(taskColumns, 'move_case_tasks', 'title', 'text')},
        ${selectColumn(taskColumns, 'move_case_tasks', 'description', 'text')},

        ${selectColumn(taskColumns, 'move_case_tasks', 'title_de', 'text')},
        ${selectColumn(taskColumns, 'move_case_tasks', 'title_fr', 'text')},
        ${selectColumn(taskColumns, 'move_case_tasks', 'title_en', 'text')},

        ${selectColumn(taskColumns, 'move_case_tasks', 'description_de', 'text')},
        ${selectColumn(taskColumns, 'move_case_tasks', 'description_fr', 'text')},
        ${selectColumn(taskColumns, 'move_case_tasks', 'description_en', 'text')},

        ${selectColumn(taskColumns, 'move_case_tasks', 'status', 'text')},
        ${selectColumn(taskColumns, 'move_case_tasks', 'due_date', 'text')},
        ${selectColumn(taskColumns, 'move_case_tasks', 'sort_order', 'integer')},
        ${selectColumn(taskColumns, 'move_case_tasks', 'external_url', 'text')},
        ${selectColumn(taskColumns, 'move_case_tasks', 'link_label', 'text')},
        ${selectColumn(taskColumns, 'move_case_tasks', 'is_required', 'boolean')},
        ${selectColumn(taskColumns, 'move_case_tasks', 'is_city_specific', 'boolean')},

        ${selectColumn(taskColumns, 'move_case_tasks', 'action_type', 'text')},
        ${selectColumn(taskColumns, 'move_case_tasks', 'action_payload', 'jsonb')},
        ${selectColumn(taskColumns, 'move_case_tasks', 'secondary_action_type', 'text')},
        ${selectColumn(taskColumns, 'move_case_tasks', 'secondary_action_payload', 'jsonb')},
        ${selectColumn(taskColumns, 'move_case_tasks', 'action_status', 'text')},

        ${selectColumn(taskColumns, 'move_case_tasks', 'completed_at', 'text')},
        ${selectColumn(taskColumns, 'move_case_tasks', 'created_at', 'text')},
        ${selectColumn(taskColumns, 'move_case_tasks', 'updated_at', 'text')}
      FROM public.move_case_tasks
      WHERE case_id = $1
        AND status IN ('open', 'done')
      ORDER BY sort_order ASC, created_at ASC
    `,
    [caseId]
  );

  return {
    tasks: result.rows.map(mapMoveCaseTask),
  };
}

export async function getMoveCaseTaskDetail(
  caseId: string,
  taskId: string,
  userId: number | string
): Promise<{ task: MoveCaseTaskDetailDto; nextTaskId: string | null }> {
  if (!caseId?.trim()) {
    throw new Error('Case id is required');
  }

  if (!taskId?.trim()) {
    throw new Error('Task id is required');
  }

  const ownershipResult = await pool.query<IdRow>(
    `
      SELECT id
      FROM public.move_cases
      WHERE id = $1
        AND user_id = $2
      LIMIT 1
    `,
    [caseId, userId]
  );

  if (!ownershipResult.rowCount) {
    throw new Error('Move case not found');
  }

  const taskRow = await loadOwnedMoveTaskRow(pool, caseId, taskId, userId);
  const baseTask = mapMoveCaseTask(taskRow);

  const [questionRows, answerRows, outputRows, entityRows, nextTaskId] =
    await Promise.all([
      taskRow.template_id
        ? loadOptionalTableRows(
            'public.move_task_template_questions',
            'task_template_id',
            taskRow.template_id
          )
        : Promise.resolve([]),
      loadOptionalTableRows('public.move_case_task_answers', 'case_task_id', taskId),
      loadOptionalTableRows('public.move_case_task_outputs', 'case_task_id', taskId),
      loadOptionalTableRows('public.move_case_task_entities', 'case_task_id', taskId),
      findNextRelevantMoveTaskId(caseId, taskId),
    ]);

  const questions = questionRows
    .map(mapQuestionRow)
    .sort((a, b) => a.sort_order - b.sort_order);
  const answers = answerRows.map(mapAnswerRow);
  const outputs = outputRows.map(mapOutputRow);
  const entities = entityRows.map(mapEntityRow);

  return {
    task: {
      ...baseTask,
      header: taskRow.header,
      header_de: taskRow.header_de,
      header_fr: taskRow.header_fr,
      header_en: taskRow.header_en,
      city_service: taskRow.city_service_id
        ? {
            office_name: taskRow.office_name ?? null,
            office_address: taskRow.office_address ?? null,
            office_email: taskRow.office_email ?? null,
            office_phone: taskRow.office_phone ?? null,
            website_url: taskRow.website_url ?? null,
            online_available: Boolean(taskRow.online_available),
            appointment_required: Boolean(taskRow.appointment_required),
          }
        : null,
      questions,
      answers,
      outputs,
      entities,
      primary_action: taskRow.action_type
        ? {
            type: normalizeActionType(taskRow.action_type),
            payload: (taskRow.action_payload ?? null) as Record<string, unknown> | null,
          }
        : null,
      secondary_action: taskRow.secondary_action_type
        ? {
            type: normalizeActionType(taskRow.secondary_action_type),
            payload: (taskRow.secondary_action_payload ?? null) as Record<string, unknown> | null,
          }
        : null,
      form_completion: buildFormCompletionSummary(questions, answers),
    },
    nextTaskId,
  };
}

export async function saveMoveCaseTaskAnswers(
  caseId: string,
  taskId: string,
  userId: number | string,
  answers: SaveMoveTaskAnswerInput[]
): Promise<{ task: MoveCaseTaskDetailDto; nextTaskId: string | null }> {
  if (!caseId?.trim()) {
    throw new Error('Case id is required');
  }

  if (!taskId?.trim()) {
    throw new Error('Task id is required');
  }

  const normalizedAnswers = (answers ?? [])
    .map((item) => ({
      question_key: asString(item?.question_key),
      answer: toJsonValue(item?.answer),
    }))
    .filter(
      (item): item is { question_key: string; answer: unknown } =>
        Boolean(item.question_key)
    );

  if (normalizedAnswers.length === 0) {
    throw new Error('Answers are required');
  }

  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    const ownershipResult = await client.query<IdRow>(
      `
        SELECT mc.id
        FROM public.move_cases mc
        INNER JOIN public.move_case_tasks t
          ON t.case_id = mc.id
        WHERE mc.id = $1
          AND t.id = $2
          AND mc.user_id = $3
        LIMIT 1
      `,
      [caseId, taskId, userId]
    );

    if (!ownershipResult.rowCount) {
      throw new Error('Move task not found');
    }

    const taskResult = await client.query<MoveCaseTaskDetailRow>(
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
        WHERE id = $1
          AND case_id = $2
        LIMIT 1
      `,
      [taskId, caseId]
    );

    if (!taskResult.rowCount) {
      throw new Error('Move task not found');
    }

    for (const item of normalizedAnswers) {
      const answerJson =
        item.answer === undefined ? null : JSON.stringify(item.answer);

      const updateResult = await client.query(
        `
          UPDATE public.move_case_task_answers
          SET
            answer_json = $1::jsonb,
            answered_at = NOW(),
            updated_at = NOW()
          WHERE case_task_id = $2
            AND question_key = $3
        `,
        [answerJson, taskId, item.question_key]
      );

      if (updateResult.rowCount && updateResult.rowCount > 0) {
        continue;
      }

      await client.query(
        `
          INSERT INTO public.move_case_task_answers (
            case_task_id,
            question_key,
            answer_json,
            answered_at,
            created_at,
            updated_at
          )
          VALUES ($1, $2, $3::jsonb, NOW(), NOW(), NOW())
        `,
        [taskId, item.question_key, answerJson]
      );
    }

    await client.query(
      `
        UPDATE public.move_case_tasks
        SET
          updated_at = NOW()
        WHERE id = $1
          AND case_id = $2
      `,
      [taskId, caseId]
    );

    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }

  await refreshMoveCaseTasks(caseId);

  return getMoveCaseTaskDetail(caseId, taskId, userId);
}

export async function executeMoveCaseTaskAction(
  caseId: string,
  taskId: string,
  actionType: string,
  userId: number | string
): Promise<ExecuteMoveTaskActionResult> {
  if (!caseId?.trim()) {
    throw new Error('Case id is required');
  }

  if (!taskId?.trim()) {
    throw new Error('Task id is required');
  }

  const normalizedActionType = normalizeActionType(actionType);

  if (!isSupportedMoveTaskActionType(normalizedActionType)) {
    throw new Error('Unsupported move task action');
  }

  const beforeDetail = await getMoveCaseTaskDetail(caseId, taskId, userId);
  const selectedAction = resolveTaskAction(beforeDetail.task, normalizedActionType);
  const moveCase = await loadMoveCaseContext(caseId);

  const languageAnswer =
    beforeDetail.task.answers.find((answer) => answer.question_key === 'language') ??
    beforeDetail.task.answers.find((answer) => answer.question_key === 'language_code');

  const languageCode = normalizeLanguageCode(
    languageAnswer?.value ?? languageAnswer?.value_text ?? 'de'
  );

  const actionContext = buildActionContext(
    beforeDetail.task,
    moveCase,
    languageCode
  );

  const client = await pool.connect();

  let outputKey = '';
  let outputType = '';
  let outputTitle: string | null = null;

  try {
    await client.query('BEGIN');

    await loadOwnedMoveTaskRow(client, caseId, taskId, userId);

    let actionData: Record<string, unknown>;

    if (normalizedActionType === 'web') {
      actionData = buildWebActionData(beforeDetail.task, selectedAction);
      outputKey = 'link_opened';
      outputType = 'link_opened';
      outputTitle = 'Link geöffnet';
    } else if (normalizedActionType === 'email') {
      actionData = await buildEmailActionData(actionContext, selectedAction);
      outputKey = 'email_draft';
      outputType = 'email_draft';
      outputTitle = 'E-Mail Entwurf';
    } else if (normalizedActionType === 'copy_text') {
      actionData = await buildCopyTextActionData(actionContext, selectedAction);
      outputKey = 'copy_text';
      outputType = 'copy_text';
      outputTitle = 'Kopiertext';
    } else if (normalizedActionType === 'whatsapp') {
      actionData = buildWhatsappActionData(actionContext, selectedAction);
      outputKey = 'whatsapp_draft';
      outputType = 'whatsapp_draft';
      outputTitle = 'WhatsApp Entwurf';
    } else {
      actionData = await buildPdfActionData(actionContext, selectedAction, taskId);
      outputKey = 'pdf_document';
      outputType = 'pdf';
      outputTitle = 'PDF Dokument';
    }

    await saveMoveTaskOutput(
      client,
      taskId,
      outputKey,
      outputType,
      outputTitle,
      actionData
    );

    await client.query(
      `
        UPDATE public.move_case_tasks
        SET
          action_status = 'completed',
          updated_at = NOW()
        WHERE id = $1
          AND case_id = $2
      `,
      [taskId, caseId]
    );

    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }

  const detail = await getMoveCaseTaskDetail(caseId, taskId, userId);
  const createdOutput =
    [...detail.task.outputs].reverse().find((output) => output.type === outputType) ??
    null;

  return {
    action: {
      type: normalizedActionType,
      status: 'prepared',
      data:
        createdOutput?.content && typeof createdOutput.content === 'object'
          ? (createdOutput.content as Record<string, unknown>)
          : {},
      output: createdOutput,
    },
    task: detail.task,
    nextTaskId: detail.nextTaskId,
  };
}

export async function completeMoveCaseTask(
  caseId: string,
  taskId: string,
  userId: number | string
): Promise<{ task: MoveCaseTaskDetailDto; nextTaskId: string | null }> {
  if (!caseId?.trim()) {
    throw new Error('Case id is required');
  }

  if (!taskId?.trim()) {
    throw new Error('Task id is required');
  }

  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    await loadOwnedMoveTaskRow(client, caseId, taskId, userId);

    const updateResult = await client.query<IdRow>(
      `
        UPDATE public.move_case_tasks
        SET
          status = 'done',
          completed_at = NOW(),
          updated_at = NOW()
        WHERE id = $1
          AND case_id = $2
        RETURNING id
      `,
      [taskId, caseId]
    );

    if (!updateResult.rowCount) {
      throw new Error('Move task not found');
    }

    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }

  await refreshMoveCaseTasks(caseId);

  const detail = await getMoveCaseTaskDetail(caseId, taskId, userId);

  return {
    task: detail.task,
    nextTaskId: await findNextRelevantMoveTaskId(caseId, taskId),
  };
}