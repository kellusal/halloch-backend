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
  const result = await pool.query<ProfileRow>(
    `
    SELECT
      u.id,
      u.email,
      u.is_activated,
      u.is_active,
      u.first_name,
      u.last_name,
      u.date_of_birth,
      u.nationality,
      u.marital_status,
      u.language,
      u.phone,
      u.street,
      u.zip,
      u.city,
      u.canton,

      p.has_car,
      p.has_children,
      p.has_dog,
      p.children_count,
      p.health_insurance_name,
      p.employer_name

    FROM app.users u
    LEFT JOIN app.user_profiles p
      ON p.user_id = u.id
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

    const current = await getMyProfile(userId);

    // USERS TABLE
    await client.query(
      `
      UPDATE app.users
      SET
        last_name = $1,
        date_of_birth = $2,
        nationality = $3,
        marital_status = $4,
        phone = $5,
        street = $6,
        zip = $7,
        city = $8,
        canton = $9
      WHERE id = $10
      `,
      [
        input.lastName ?? current.personal.lastName ?? null,
        input.dateOfBirth ?? current.personal.dateOfBirth ?? null,
        input.nationality ?? current.personal.nationality ?? null,
        input.maritalStatus ?? current.personal.maritalStatus ?? null,
        input.phone ?? current.contact.phone ?? null,
        input.street ?? current.contact.street ?? null,
        input.zip ?? current.contact.zip ?? null,
        input.city ?? current.contact.city ?? null,
        input.canton ?? current.contact.canton ?? null,
        userId,
      ]
    );

    const nextHasChildren =
      typeof input.hasChildren === 'boolean'
        ? input.hasChildren
        : current.household.hasChildren;

    // USER_PROFILES TABLE (UPSERT)
    await client.query(
      `
      INSERT INTO app.user_profiles (
        user_id,
        has_car,
        has_children,
        has_dog,
        children_count,
        health_insurance_name,
        employer_name,
        created_at,
        updated_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())
      ON CONFLICT (user_id)
      DO UPDATE SET
        has_car = EXCLUDED.has_car,
        has_children = EXCLUDED.has_children,
        has_dog = EXCLUDED.has_dog,
        children_count = EXCLUDED.children_count,
        health_insurance_name = EXCLUDED.health_insurance_name,
        employer_name = EXCLUDED.employer_name,
        updated_at = NOW()
      `,
      [
        userId,
        input.hasCar ?? current.household.hasCar,
        nextHasChildren,
        input.hasDog ?? current.household.hasDog,
        nextHasChildren === false
          ? 0
          : input.childrenCount ?? current.household.childrenCount,
        input.healthInsuranceName ?? current.organizations.healthInsuranceName,
        input.employerName ?? current.organizations.employerName,
      ]
    );

    await client.query('COMMIT');

    return getMyProfile(userId);
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}