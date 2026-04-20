import { pool } from '../../db/pool';

export type ProfileRow = {
  id: string;
  email: string;
  is_activated: boolean | null;
  is_active: boolean | null;
  first_name: string | null;
  last_name: string | null;
  date_of_birth: string | null;
  nationality: string | null;
  marital_status: string | null;
  language: string | null;
  phone: string | null;
  street: string | null;
  zip: string | null;
  city: string | null;
  canton: string | null;

  has_car: boolean | null;
  has_children: boolean | null;
  has_dog: boolean | null;
  children_count: number | null;
  health_insurance_name: string | null;
  employer_name: string | null;
};

function normalizeLanguage(value: string | null | undefined): 'de' | 'fr' | 'en' {
  const normalized = String(value ?? 'de').toLowerCase();

  if (normalized.startsWith('fr')) return 'fr';
  if (normalized.startsWith('en')) return 'en';
  return 'de';
}

const schemaWarningCache = new Set<string>();

function warnSchemaFallback(key: string, message: string, error?: unknown) {
  if (schemaWarningCache.has(key)) return;
  schemaWarningCache.add(key);

  const details =
    error instanceof Error && error.message ? ` ${error.message}` : '';

  console.warn(`[profile] ${message}${details}`);
}

async function loadTableColumns(schemaName: string, tableName: string) {
  try {
    const result = await pool.query<{ column_name: string }>(
      `
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema = $1
        AND table_name = $2
      `,
      [schemaName, tableName]
    );

    return new Set(result.rows.map((row) => row.column_name));
  } catch (error) {
    warnSchemaFallback(
      `profile-columns:${schemaName}.${tableName}`,
      `Could not inspect ${schemaName}.${tableName}; using empty column set.`,
      error
    );
    return new Set<string>();
  }
}

function buildSelectableColumn(
  columns: Set<string>,
  columnName: string,
  aliasPrefix: string
) {
  if (columns.has(columnName)) {
    return `${aliasPrefix}.${columnName}`;
  }

  return `NULL::text AS ${columnName}`;
}

function mapProfile(row: ProfileRow) {
  return {
    id: String(row.id),
    auth: {
      email: String(row.email ?? ''),
      isActivated: Boolean(row.is_activated ?? row.is_active ?? false),
    },
    personal: {
      firstName: row.first_name ?? '',
      lastName: row.last_name ?? '',
      dateOfBirth: row.date_of_birth ?? null,
      nationality: row.nationality ?? null,
      maritalStatus: row.marital_status ?? null,
      language: normalizeLanguage(row.language),
    },
    contact: {
      email: String(row.email ?? ''),
      phone: row.phone ?? null,
      street: row.street ?? null,
      zip: row.zip ?? null,
      city: row.city ?? null,
      canton: row.canton ?? null,
    },
    household: {
      hasCar: Boolean(row.has_car ?? false),
      hasChildren: Boolean(row.has_children ?? false),
      hasDog: Boolean(row.has_dog ?? false),
      childrenCount: Number(row.children_count ?? 0),
    },
    organizations: {
      healthInsuranceName: row.health_insurance_name ?? null,
      employerName: row.employer_name ?? null,
    },
  };
}

export async function getMyProfile(userId: string) {
  const [userColumns, profileColumns] = await Promise.all([
    loadTableColumns('app', 'users'),
    loadTableColumns('app', 'user_profiles'),
  ]);
  const profileJoin =
    profileColumns.size > 0 && profileColumns.has('user_id')
      ? 'LEFT JOIN app.user_profiles p ON p.user_id = u.id'
      : '';

  if (!profileJoin) {
    warnSchemaFallback(
      'profile-user_profiles-read',
      'app.user_profiles is unavailable for profile reads; household/profile extension fields will be empty.'
    );
  }

  const result = await pool.query<ProfileRow>(
    `
      SELECT
        u.id,
        u.email,
        ${buildSelectableColumn(userColumns, 'is_activated', 'u')},
        ${buildSelectableColumn(userColumns, 'is_active', 'u')},
        ${buildSelectableColumn(userColumns, 'first_name', 'u')},
        ${buildSelectableColumn(userColumns, 'last_name', 'u')},
        ${buildSelectableColumn(userColumns, 'date_of_birth', 'u')},
        ${buildSelectableColumn(userColumns, 'nationality', 'u')},
        ${buildSelectableColumn(userColumns, 'marital_status', 'u')},
        ${buildSelectableColumn(userColumns, 'language', 'u')},
        ${buildSelectableColumn(userColumns, 'phone', 'u')},
        ${buildSelectableColumn(userColumns, 'street', 'u')},
        ${buildSelectableColumn(userColumns, 'zip', 'u')},
        ${buildSelectableColumn(userColumns, 'city', 'u')},
        ${buildSelectableColumn(userColumns, 'canton', 'u')},

        ${buildSelectableColumn(profileColumns, 'has_car', 'p')},
        ${buildSelectableColumn(profileColumns, 'has_children', 'p')},
        ${buildSelectableColumn(profileColumns, 'has_dog', 'p')},
        ${buildSelectableColumn(profileColumns, 'children_count', 'p')},
        ${buildSelectableColumn(profileColumns, 'health_insurance_name', 'p')},
        ${buildSelectableColumn(profileColumns, 'employer_name', 'p')}

      FROM app.users u
      ${profileJoin}
      WHERE u.id = $1
      LIMIT 1
    `,
    [userId]
  );

  if (!result.rows[0]) {
    throw new Error('User not found');
  }

  return mapProfile(result.rows[0]);
}

export type UpdateProfileInput = {
  lastName?: string;
  dateOfBirth?: string | null;
  nationality?: string | null;
  maritalStatus?: string | null;

  phone?: string | null;
  street?: string | null;
  zip?: string | null;
  city?: string | null;
  canton?: string | null;

  hasCar?: boolean;
  hasChildren?: boolean;
  hasDog?: boolean;
  childrenCount?: number;

  healthInsuranceName?: string | null;
  employerName?: string | null;
};

export async function updateMyProfile(userId: string, input: UpdateProfileInput) {
  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    const [userColumns, profileColumns] = await Promise.all([
      loadTableColumns('app', 'users'),
      loadTableColumns('app', 'user_profiles'),
    ]);

    const current = await getMyProfile(userId);

    // USERS TABLE
    const userAssignments: string[] = [];
    const userValues: unknown[] = [];

    const addUserAssignment = (column: string, value: unknown) => {
      if (!userColumns.has(column)) return;
      userAssignments.push(`${column} = $${userValues.length + 1}`);
      userValues.push(value);
    };

    addUserAssignment('last_name', input.lastName ?? current.personal.lastName ?? null);
    addUserAssignment(
      'date_of_birth',
      input.dateOfBirth ?? current.personal.dateOfBirth ?? null
    );
    addUserAssignment(
      'nationality',
      input.nationality ?? current.personal.nationality ?? null
    );
    addUserAssignment(
      'marital_status',
      input.maritalStatus ?? current.personal.maritalStatus ?? null
    );
    addUserAssignment('phone', input.phone ?? current.contact.phone ?? null);
    addUserAssignment('street', input.street ?? current.contact.street ?? null);
    addUserAssignment('zip', input.zip ?? current.contact.zip ?? null);
    addUserAssignment('city', input.city ?? current.contact.city ?? null);
    addUserAssignment('canton', input.canton ?? current.contact.canton ?? null);

    if (userAssignments.length > 0) {
      await client.query(
        `
        UPDATE app.users
        SET ${userAssignments.join(', ')}
        WHERE id = $${userValues.length + 1}
        `,
        [...userValues, userId]
      );
    } else {
      warnSchemaFallback(
        'profile-users-update-columns',
        'No writable profile columns found on app.users; user update skipped.'
      );
    }

    const nextHasChildren =
      typeof input.hasChildren === 'boolean'
        ? input.hasChildren
        : current.household.hasChildren;

    // USER_PROFILES TABLE (UPSERT)
    if (profileColumns.has('user_id')) {
      const insertColumns: string[] = ['user_id'];
      const parameterValues: unknown[] = [userId];
      const updateAssignments: string[] = [];

      const addProfileColumn = (column: string, value: unknown) => {
        if (!profileColumns.has(column)) return;
        insertColumns.push(column);
        parameterValues.push(value);
        updateAssignments.push(`${column} = EXCLUDED.${column}`);
      };

      addProfileColumn('has_car', input.hasCar ?? current.household.hasCar);
      addProfileColumn('has_children', nextHasChildren);
      addProfileColumn('has_dog', input.hasDog ?? current.household.hasDog);
      addProfileColumn(
        'children_count',
        nextHasChildren === false
          ? 0
          : input.childrenCount ?? current.household.childrenCount
      );
      addProfileColumn(
        'health_insurance_name',
        input.healthInsuranceName ?? current.organizations.healthInsuranceName
      );
      addProfileColumn(
        'employer_name',
        input.employerName ?? current.organizations.employerName
      );

      if (profileColumns.has('created_at')) {
        insertColumns.push('created_at');
      }

      if (profileColumns.has('updated_at')) {
        insertColumns.push('updated_at');
        updateAssignments.push('updated_at = NOW()');
      }

      let parameterIndex = 0;
      const placeholders = insertColumns.map((columnName) => {
        if (columnName === 'created_at' || columnName === 'updated_at') {
          return 'NOW()';
        }
        parameterIndex += 1;
        return `$${parameterIndex}`;
      });

      await client.query(
        `
        INSERT INTO app.user_profiles (${insertColumns.join(', ')})
        VALUES (${placeholders.join(', ')})
        ON CONFLICT (user_id)
        DO UPDATE SET
          ${updateAssignments.length ? updateAssignments.join(', ') : 'user_id = EXCLUDED.user_id'}
        `,
        parameterValues
      );
    } else {
      warnSchemaFallback(
        'profile-user_profiles-user_id',
        'app.user_profiles.user_id is unavailable; profile household upsert skipped.'
      );
    }

    await client.query('COMMIT');

    return getMyProfile(userId);
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}
