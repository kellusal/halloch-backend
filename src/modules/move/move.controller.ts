import { NextFunction, Request, Response } from 'express';
import { AppError } from '../../middleware/error.middleware';
import * as moveService from './move.services';

type CaseParams = {
  caseId: string;
};

function mapMoveServiceError(error: unknown) {
  const message =
    error instanceof Error ? error.message : 'Internal server error';

  if (
    message === 'User is required' ||
    message === 'To city is required' ||
    message === 'Move date is required' ||
    message === 'Destination city not found' ||
    message === 'Origin city not found'
  ) {
    return new AppError(message, 400);
  }

  if (message === 'Move case not found') {
    return new AppError(message, 404);
  }

  return error instanceof Error
    ? error
    : new AppError('Internal server error', 500, { expose: false });
}

export async function createMoveCase(
  req: Request,
  res: Response,
  next: NextFunction
) {
  try {
    if (!req.user?.id) {
      return next(new AppError('Unauthorized', 401));
    }

    const result = await moveService.createMoveCase({
      userId: Number(req.user.id),
      fromCity: req.body.fromCity,
      toCity: req.body.toCity,
      moveDate: req.body.moveDate,
      hasCar: Boolean(req.body.hasCar),
      hasChildren: Boolean(req.body.hasChildren),
    });

    return res.status(201).json(result);
  } catch (error) {
    return next(mapMoveServiceError(error));
  }
}

export async function getMoveCaseById(
  req: Request<CaseParams>,
  res: Response,
  next: NextFunction
) {
  try {
    if (!req.user?.id) {
      return next(new AppError('Unauthorized', 401));
    }

    const { caseId } = req.params;

    const result = await moveService.getMoveCaseById(caseId, Number(req.user.id));

    return res.status(200).json(result);
  } catch (error) {
    return next(mapMoveServiceError(error));
  }
}

export async function getMoveCaseTasks(
  req: Request<CaseParams>,
  res: Response,
  next: NextFunction
) {
  try {
    if (!req.user?.id) {
      return next(new AppError('Unauthorized', 401));
    }

    const { caseId } = req.params;

    const result = await moveService.getMoveCaseTasks(caseId, Number(req.user.id));

    return res.status(200).json(result);
  } catch (error) {
    return next(mapMoveServiceError(error));
  }
}
