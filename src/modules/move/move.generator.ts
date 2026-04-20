import { generateMoveCaseTasks } from './move.case-task-generator';

export async function generateTasksForCase(caseId: string) {
  await generateMoveCaseTasks(caseId);
}
