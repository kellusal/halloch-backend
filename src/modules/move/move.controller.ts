import { Request, Response } from 'express';
import * as moveService from './move.services';

type CaseParams = {
  caseId: string;
};

export async function createMoveCase(req: Request, res: Response) {
  try {
    if (!req.user?.id) {
      return res.status(401).json({ message: 'Unauthorized' });
    }

    const result = await moveService.createMoveCase({
      userId: req.user.id,
      fromCity: req.body.fromCity,
      toCity: req.body.toCity,
      moveDate: req.body.moveDate,
      hasCar: Boolean(req.body.hasCar),
      hasChildren: Boolean(req.body.hasChildren),
    });

    return res.status(201).json(result);
  } catch (error) {
    const message =
      error instanceof Error ? error.message : 'Could not create move case';

    if (
      message === 'User is required' ||
      message === 'To city is required' ||
      message === 'Move date is required' ||
      message === 'Destination city not found' ||
      message === 'Origin city not found'
    ) {
      return res.status(400).json({ message });
    }

    return res.status(500).json({ message: 'Internal server error' });
  }
}

export async function getMoveCaseById(
  req: Request<CaseParams>,
  res: Response
) {
  try {
    if (!req.user?.id) {
      return res.status(401).json({ message: 'Unauthorized' });
    }

    const { caseId } = req.params;

    const result = await moveService.getMoveCaseById(caseId, req.user.id);

    return res.status(200).json(result);
  } catch (error) {
    const message =
      error instanceof Error ? error.message : 'Could not load move case';

    if (message === 'Move case not found') {
      return res.status(404).json({ message });
    }

    return res.status(500).json({ message: 'Internal server error' });
  }
}

export async function getMoveCaseTasks(
  req: Request<CaseParams>,
  res: Response
) {
  try {
    if (!req.user?.id) {
      return res.status(401).json({ message: 'Unauthorized' });
    }

    const { caseId } = req.params;

    const result = await moveService.getMoveCaseTasks(caseId, req.user.id);

    return res.status(200).json(result);
  } catch (error) {
    const message =
      error instanceof Error ? error.message : 'Could not load move case tasks';

    if (message === 'Move case not found') {
      return res.status(404).json({ message });
    }

    return res.status(500).json({ message: 'Internal server error' });
  }
}