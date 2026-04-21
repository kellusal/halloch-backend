import { pool } from '../../db/pool';
import { generateMoveCaseTasks } from './move.case-task-generator';

export async function refreshMoveCaseTasks(caseId: string) {
  await generateMoveCaseTasks(String(caseId));
}

export async function refreshActiveMoveCasesForUser(userId: number | string) {
  const result = await pool.query<{ id: string }>(
    `
    SELECT id
    FROM move_cases
    WHERE user_id = $1
      AND status = 'draft'
    ORDER BY created_at DESC
    `,
    [userId]
  );

  for (const row of result.rows) {
    await refreshMoveCaseTasks(String(row.id));
  }
}