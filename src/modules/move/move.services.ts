import { pool } from '../../db/pool';
import { generateTasksForCase } from './move.generator';
import { refreshMoveCaseTasks } from './move.task-sync';

type CreateMoveCaseInput = {
  userId: number;
  fromCity?: string;
  toCity: string;
  moveDate: string;
  hasCar: boolean;
  hasChildren: boolean;
};

type CityRow = {
  id: string;
  name: string;
};

type MoveCaseRow = {
  id: string;
  user_id: string;
  from_city_id: string | null;
  to_city_id: string;
  move_date: string;
  has_car: boolean;
  has_children: boolean;
  status: string;
  created_at: string;
  updated_at: string;
};

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

type SupportedMoveTaskActionType = 'web' | 'email' | 'copy_text' | 'whatsapp';

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

  console.warn(`[move] ${message}${details}`);
}

function mapMoveCase(row: MoveCaseRow) {
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

function mapQuestionOption(option: unknown) {
  if (option && typeof option === 'object' && !Array.isArray(option)) {
    const record = option as Record<string, unknown>;
    const labelI18n = record.label && typeof record.label === 'object'
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
    description: localizedScalar(descriptionI18n, asString(pickFirst(row, ['description']))),
    placeholder: localizedScalar(placeholderI18n, asString(pickFirst(row, ['placeholder']))),
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
  const value = parseJsonLike(pickFirst(row, ['answer_json', 'value_json', 'answer', 'value']));

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
  const titleI18n = localizedValue(row, 'title');

  return {
    id: asString(pickFirst(row, ['id'])),
    output_key: asString(pickFirst(row, ['output_key', 'key', 'slug'])),
    type: asString(pickFirst(row, ['output_type', 'type', 'kind'])),
    title: localizedScalar(titleI18n, asString(pickFirst(row, ['title']))),
    content: parseJsonLike(pickFirst(row, ['content_json', 'payload_json', 'content', 'payload'])),
    created_at: asString(pickFirst(row, ['created_at'])),
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
  if (typeof value === 'object') return Object.keys(value as Record<string, unknown>).length > 0;
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

async function loadOptionalTableRows(tableName: string, whereColumn: string, whereValue: string) {
  try {
    const result = await pool.query<GenericRow>(
      `SELECT * FROM ${tableName} WHERE ${whereColumn} = $1 ORDER BY created_at ASC, id ASC`,
      [whereValue]
    );

    return result.rows;
  } catch (error) {
    const message = error instanceof Error ? error.message : '';
    const code = typeof error === 'object' && error !== null && 'code' in error
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

function toJsonValue(value: unknown): unknown {
  if (value === undefined) return null;
  return value;
}

function buildInsertStatementParts(columns: string[], parameterColumns: string[]) {
  let parameterIndex = 0;

  const placeholders = columns.map((column) => {
    if (column === 'answered_at' || column === 'created_at' || column === 'updated_at') {
      return 'NOW()';
    }

    parameterIndex += 1;
    return `$${parameterIndex}`;
  });

  return {
    placeholders,
    expectedParameterCount: parameterColumns.length,
  };
}

function isSupportedMoveTaskActionType(value: string): value is SupportedMoveTaskActionType {
  return value === 'web' || value === 'email' || value === 'copy_text' || value === 'whatsapp';
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
        AND status IN ('in_progress', 'open')
        AND ($2::uuid IS NULL OR id <> $2::uuid)
      ORDER BY
        CASE
          WHEN status = 'in_progress' THEN 0
          WHEN status = 'open' THEN 1
          ELSE 2
        END,
        sort_order ASC,
        created_at ASC
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
  const taskResult = await client.query<MoveCaseTaskDetailRow & CityServiceDetailRow>(
    `
      SELECT
        t.id,
        t.case_id,
        t.template_id,
        t.city_service_id,
        t.category,

        t.header,
        t.header_de,
        t.header_fr,
        t.header_en,

        t.title,
        t.description,

        t.title_de,
        t.title_fr,
        t.title_en,

        t.description_de,
        t.description_fr,
        t.description_en,

        t.status,
        t.due_date,
        t.sort_order,
        t.external_url,
        t.link_label,
        t.is_required,
        t.is_city_specific,

        t.action_type,
        t.action_payload,
        t.secondary_action_type,
        t.secondary_action_payload,
        t.action_status,

        t.completed_at,
        t.created_at,
        t.updated_at,

        cs.office_name,
        cs.office_address,
        cs.office_email,
        cs.office_phone,
        cs.website_url,
        cs.online_available,
        cs.appointment_required
      FROM public.move_case_tasks t
      INNER JOIN public.move_cases mc
        ON mc.id = t.case_id
      LEFT JOIN public.move_city_services cs
        ON cs.id = t.city_service_id
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

async function saveMoveTaskOutput(
  client: { query: typeof pool.query },
  caseTaskId: string,
  outputKey: string,
  outputType: string,
  title: string | null,
  content: Record<string, unknown>
): Promise<void> {
  const outputColumns = await loadTableColumns(client, 'public', 'move_case_task_outputs');
  const outputJson = toJsonValue(content);
  const updateAssignments: string[] = [];
  const updateValues: unknown[] = [];

  if (outputColumns.has('output_type')) {
    updateAssignments.push(`output_type = $${updateValues.length + 1}`);
    updateValues.push(outputType);
  }

  if (outputColumns.has('type')) {
    updateAssignments.push(`type = $${updateValues.length + 1}`);
    updateValues.push(outputType);
  }

  if (outputColumns.has('title')) {
    updateAssignments.push(`title = $${updateValues.length + 1}`);
    updateValues.push(title);
  }

  if (outputColumns.has('title_de')) {
    updateAssignments.push(`title_de = $${updateValues.length + 1}`);
    updateValues.push(title);
  }

  if (outputColumns.has('content_json')) {
    updateAssignments.push(`content_json = $${updateValues.length + 1}`);
    updateValues.push(outputJson);
  }

  if (outputColumns.has('payload_json')) {
    updateAssignments.push(`payload_json = $${updateValues.length + 1}`);
    updateValues.push(outputJson);
  }

  if (outputColumns.has('content')) {
    updateAssignments.push(`content = $${updateValues.length + 1}`);
    updateValues.push(outputJson);
  }

  if (outputColumns.has('payload')) {
    updateAssignments.push(`payload = $${updateValues.length + 1}`);
    updateValues.push(outputJson);
  }

  if (outputColumns.has('updated_at')) {
    updateAssignments.push('updated_at = NOW()');
  }

  if (updateAssignments.length === 0) {
    updateAssignments.push('output_key = output_key');
  }

  const updateResult = await client.query(
    `
      UPDATE public.move_case_task_outputs
      SET ${updateAssignments.join(', ')}
      WHERE case_task_id = $${updateValues.length + 1}
        AND output_key = $${updateValues.length + 2}
    `,
    [...updateValues, caseTaskId, outputKey]
  );

  if (updateResult.rowCount && updateResult.rowCount > 0) {
    return;
  }

  const insertColumns = ['case_task_id', 'output_key'];
  const insertValueColumns = ['case_task_id', 'output_key'];
  const insertValues: unknown[] = [caseTaskId, outputKey];

  if (outputColumns.has('output_type')) {
    insertColumns.push('output_type');
    insertValueColumns.push('output_type');
    insertValues.push(outputType);
  }

  if (outputColumns.has('type')) {
    insertColumns.push('type');
    insertValueColumns.push('type');
    insertValues.push(outputType);
  }

  if (outputColumns.has('title')) {
    insertColumns.push('title');
    insertValueColumns.push('title');
    insertValues.push(title);
  }

  if (outputColumns.has('title_de')) {
    insertColumns.push('title_de');
    insertValueColumns.push('title_de');
    insertValues.push(title);
  }

  if (outputColumns.has('content_json')) {
    insertColumns.push('content_json');
    insertValueColumns.push('content_json');
    insertValues.push(outputJson);
  }

  if (outputColumns.has('payload_json')) {
    insertColumns.push('payload_json');
    insertValueColumns.push('payload_json');
    insertValues.push(outputJson);
  }

  if (outputColumns.has('content')) {
    insertColumns.push('content');
    insertValueColumns.push('content');
    insertValues.push(outputJson);
  }

  if (outputColumns.has('payload')) {
    insertColumns.push('payload');
    insertValueColumns.push('payload');
    insertValues.push(outputJson);
  }

  if (outputColumns.has('created_at')) {
    insertColumns.push('created_at');
  }

  if (outputColumns.has('updated_at')) {
    insertColumns.push('updated_at');
  }

  const { placeholders, expectedParameterCount } = buildInsertStatementParts(
    insertColumns,
    insertValueColumns
  );

  if (expectedParameterCount !== insertValues.length) {
    throw new Error('Output insert columns could not be mapped safely');
  }

  await client.query(
    `
      INSERT INTO public.move_case_task_outputs (${insertColumns.join(', ')})
      VALUES (${placeholders.join(', ')})
    `,
    insertValues
  );
}

function resolveTaskAction(
  task: MoveCaseTaskDetailDto,
  actionType: SupportedMoveTaskActionType
): MoveTaskActionDto {
  const candidates = [task.primary_action, task.secondary_action].filter(
    (action): action is MoveTaskActionDto => Boolean(action)
  );

  const matched = candidates.find((action) => action.type === actionType);
  if (!matched) {
    throw new Error('Move task action not available');
  }

  return matched;
}

function buildActionContext(task: MoveCaseTaskDetailDto) {
  const answersByKey = new Map<string, unknown>();

  for (const answer of task.answers) {
    if (!answer.question_key) continue;
    answersByKey.set(
      answer.question_key,
      answer.value ?? answer.value_text ?? null
    );
  }

  return {
    task,
    answersByKey,
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

function buildEmailActionData(
  context: ReturnType<typeof buildActionContext>,
  action: MoveTaskActionDto
) {
  const payload = action.payload ?? {};
  const to =
    asString(payload.to) ??
    asString(context.answersByKey.get('recipient_email')) ??
    context.task.city_service?.office_email ??
    null;
  const subject =
    localizedPayloadScalar(payload.subject) ??
    `${context.task.title}`;
  const body =
    localizedPayloadScalar(payload.body) ??
    [
      context.task.description,
      context.task.city_service?.office_name,
      context.task.city_service?.office_address,
    ]
      .filter(Boolean)
      .join('\n\n');

  return {
    to,
    subject,
    body,
    label: localizedPayloadScalar(payload.label, context.task.linkLabel ?? 'E-Mail vorbereiten'),
  };
}

function buildCopyTextActionData(
  context: ReturnType<typeof buildActionContext>,
  action: MoveTaskActionDto
) {
  const payload = action.payload ?? {};
  const text =
    localizedPayloadScalar(payload.text) ??
    localizedPayloadScalar(payload.body) ??
    asString(context.answersByKey.get('copy_text')) ??
    context.task.description ??
    context.task.title;

  return {
    text,
    label: localizedPayloadScalar(payload.label, context.task.linkLabel ?? 'Text kopieren'),
  };
}

function buildWhatsappActionData(
  context: ReturnType<typeof buildActionContext>,
  action: MoveTaskActionDto
) {
  const payload = action.payload ?? {};
  const text =
    localizedPayloadScalar(payload.message) ??
    localizedPayloadScalar(payload.body) ??
    context.task.description ??
    context.task.title;

  return {
    text,
    label: localizedPayloadScalar(payload.label, 'WhatsApp vorbereiten'),
  };
}

async function findCityByName(cityName: string) {
  const value = cityName.trim();

  if (!value) {
    return null;
  }

  const result = await pool.query<CityRow>(
    `
      SELECT id, name
      FROM public.move_cities
      WHERE LOWER(name) = LOWER($1)
      LIMIT 1
    `,
    [value]
  );

  if (!result.rowCount) {
    return null;
  }

  return result.rows[0];
}

export async function createMoveCase(input: CreateMoveCaseInput) {
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

  const insertResult = await pool.query<MoveCaseRow>(
    `
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
    `,
    [
      input.userId,
      fromCity?.id ?? null,
      toCity.id,
      input.moveDate,
      input.hasCar,
      input.hasChildren,
    ]
  );

  const createdCase = insertResult.rows[0];

  await generateTasksForCase(createdCase.id);

  return {
    case: mapMoveCase(createdCase),
  };
}

export async function getMoveCaseById(caseId: string, userId: number) {
  if (!caseId?.trim()) {
    throw new Error('Case id is required');
  }

  const result = await pool.query<MoveCaseRow>(
    `
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
    `,
    [caseId, userId]
  );

  if (!result.rowCount) {
    throw new Error('Move case not found');
  }

  return {
    case: mapMoveCase(result.rows[0]),
  };
}

export async function getMoveCaseTasks(caseId: string, userId: number) {
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

  const result = await pool.query<MoveCaseTaskRow>(
    `
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
        AND status <> 'hidden'
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

  const taskResult = await pool.query<MoveCaseTaskDetailRow & CityServiceDetailRow>(
    `
      SELECT
        t.id,
        t.case_id,
        t.template_id,
        t.city_service_id,
        t.category,

        t.header,
        t.header_de,
        t.header_fr,
        t.header_en,

        t.title,
        t.description,

        t.title_de,
        t.title_fr,
        t.title_en,

        t.description_de,
        t.description_fr,
        t.description_en,

        t.status,
        t.due_date,
        t.sort_order,
        t.external_url,
        t.link_label,
        t.is_required,
        t.is_city_specific,

        t.action_type,
        t.action_payload,
        t.secondary_action_type,
        t.secondary_action_payload,
        t.action_status,

        t.completed_at,
        t.created_at,
        t.updated_at,

        cs.office_name,
        cs.office_address,
        cs.office_email,
        cs.office_phone,
        cs.website_url,
        cs.online_available,
        cs.appointment_required
      FROM public.move_case_tasks t
      LEFT JOIN public.move_city_services cs
        ON cs.id = t.city_service_id
      WHERE t.id = $1
        AND t.case_id = $2
      LIMIT 1
    `,
    [taskId, caseId]
  );

  if (!taskResult.rowCount) {
    throw new Error('Move task not found');
  }

  const taskRow = taskResult.rows[0];
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
            type: taskRow.action_type,
            payload: (taskRow.action_payload ?? null) as Record<string, unknown> | null,
          }
        : null,
      secondary_action: taskRow.secondary_action_type
        ? {
            type: taskRow.secondary_action_type,
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
    .filter((item): item is { question_key: string; answer: unknown } => Boolean(item.question_key));

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

    const task = taskResult.rows[0];
    const questionRows = task.template_id
      ? await loadOptionalTableRows(
          'public.move_task_template_questions',
          'task_template_id',
          task.template_id
        )
      : [];

    const questionByKey = new Map<string, GenericRow>();
    for (const row of questionRows) {
      const questionKey = asString(pickFirst(row, ['question_key', 'field_key', 'key', 'slug']));
      if (questionKey) {
        questionByKey.set(questionKey, row);
      }
    }

    const answerColumns = await loadTableColumns(client, 'public', 'move_case_task_answers');

    for (const item of normalizedAnswers) {
      const matchingQuestion = questionByKey.get(item.question_key) ?? null;
      const questionId = matchingQuestion
        ? asString(pickFirst(matchingQuestion, ['id']))
        : null;
      const answerJson = toJsonValue(item.answer);
      const answerText =
        typeof item.answer === 'string' ? item.answer : JSON.stringify(answerJson);

      const updateAssignments: string[] = [];
      const updateValues: unknown[] = [];

      if (answerColumns.has('question_id')) {
        updateAssignments.push(`question_id = $${updateValues.length + 1}`);
        updateValues.push(questionId);
      }

      if (answerColumns.has('answer_json')) {
        updateAssignments.push(`answer_json = $${updateValues.length + 1}`);
        updateValues.push(answerJson);
      }

      if (answerColumns.has('value_json')) {
        updateAssignments.push(`value_json = $${updateValues.length + 1}`);
        updateValues.push(answerJson);
      }

      if (answerColumns.has('value_text')) {
        updateAssignments.push(`value_text = $${updateValues.length + 1}`);
        updateValues.push(answerText);
      }

      if (answerColumns.has('answer_text')) {
        updateAssignments.push(`answer_text = $${updateValues.length + 1}`);
        updateValues.push(answerText);
      }

      if (answerColumns.has('answered_at')) {
        updateAssignments.push(`answered_at = NOW()`);
      }

      if (answerColumns.has('updated_at')) {
        updateAssignments.push(`updated_at = NOW()`);
      }

      if (updateAssignments.length === 0) {
        updateAssignments.push('question_key = question_key');
      }

      const updateWhereCaseTaskIndex = updateValues.length + 1;
      const updateWhereQuestionKeyIndex = updateValues.length + 2;

      const updateResult = await client.query(
        `
          UPDATE public.move_case_task_answers
          SET ${updateAssignments.join(', ')}
          WHERE case_task_id = $${updateWhereCaseTaskIndex}
            AND question_key = $${updateWhereQuestionKeyIndex}
        `,
        [...updateValues, taskId, item.question_key]
      );

      if (updateResult.rowCount && updateResult.rowCount > 0) {
        continue;
      }

      const insertColumns = ['case_task_id', 'question_key'];
      const insertValueColumns = ['case_task_id', 'question_key'];
      const insertValues: unknown[] = [taskId, item.question_key];

      if (answerColumns.has('question_id')) {
        insertColumns.push('question_id');
        insertValueColumns.push('question_id');
        insertValues.push(questionId);
      }

      if (answerColumns.has('answer_json')) {
        insertColumns.push('answer_json');
        insertValueColumns.push('answer_json');
        insertValues.push(answerJson);
      }

      if (answerColumns.has('value_json')) {
        insertColumns.push('value_json');
        insertValueColumns.push('value_json');
        insertValues.push(answerJson);
      }

      if (answerColumns.has('value_text')) {
        insertColumns.push('value_text');
        insertValueColumns.push('value_text');
        insertValues.push(answerText);
      }

      if (answerColumns.has('answer_text')) {
        insertColumns.push('answer_text');
        insertValueColumns.push('answer_text');
        insertValues.push(answerText);
      }

      if (answerColumns.has('answered_at')) {
        insertColumns.push('answered_at');
      }

      if (answerColumns.has('created_at')) {
        insertColumns.push('created_at');
      }

      if (answerColumns.has('updated_at')) {
        insertColumns.push('updated_at');
      }

      const { placeholders, expectedParameterCount } = buildInsertStatementParts(
        insertColumns,
        insertValueColumns
      );

      if (expectedParameterCount !== insertValues.length) {
        throw new Error('Answer insert columns could not be mapped safely');
      }

      await client.query(
        `
          INSERT INTO public.move_case_task_answers (${insertColumns.join(', ')})
          VALUES (${placeholders.join(', ')})
        `,
        insertValues
      );
    }

    await client.query(
      `
        UPDATE public.move_case_tasks
        SET
          status = CASE WHEN status = 'open' THEN 'in_progress' ELSE status END,
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

  // Keep the regeneration hook here so future runtime rules can react to saved answers.
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

  if (!isSupportedMoveTaskActionType(actionType)) {
    throw new Error('Unsupported move task action');
  }

  const beforeDetail = await getMoveCaseTaskDetail(caseId, taskId, userId);
  const selectedAction = resolveTaskAction(beforeDetail.task, actionType);
  const actionContext = buildActionContext(beforeDetail.task);
  const client = await pool.connect();
  let outputKey = '';

  try {
    await client.query('BEGIN');

    await loadOwnedMoveTaskRow(client, caseId, taskId, userId);

    let outputType = '';
    let outputTitle: string | null = null;
    let actionData: Record<string, unknown>;

    if (actionType === 'web') {
      actionData = buildWebActionData(beforeDetail.task, selectedAction);
      outputKey = 'link_opened';
      outputType = 'link_opened';
      outputTitle = 'Link geöffnet';
    } else if (actionType === 'email') {
      actionData = buildEmailActionData(actionContext, selectedAction);
      outputKey = 'email_draft';
      outputType = 'email_draft';
      outputTitle = 'E-Mail Entwurf';
    } else if (actionType === 'copy_text') {
      actionData = buildCopyTextActionData(actionContext, selectedAction);
      outputKey = 'copy_text';
      outputType = 'copy_text';
      outputTitle = 'Kopiertext';
    } else {
      actionData = buildWhatsappActionData(actionContext, selectedAction);
      outputKey = 'whatsapp_draft';
      outputType = 'whatsapp_draft';
      outputTitle = 'WhatsApp Entwurf';
    }

    await saveMoveTaskOutput(client, taskId, outputKey, outputType, outputTitle, actionData);

    await client.query(
      `
        UPDATE public.move_case_tasks
        SET
          status = CASE
            WHEN status = 'open' THEN 'in_progress'
            ELSE status
          END,
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
    detail.task.outputs.find((output) => output.output_key === outputKey) ?? null;

  return {
    action: {
      type: actionType,
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
