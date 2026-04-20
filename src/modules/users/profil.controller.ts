import { Request, Response } from 'express';
import { sendInternalServerError } from '../../middleware/error.middleware';
import { refreshActiveMoveCasesForUser } from '../move/move.task-sync';
import { getMyProfile, updateMyProfile } from './profil.repository';

function logProfileError(event: string, req: Request, error: unknown) {
  console.error(event, {
    route: `${req.method} ${req.originalUrl}`,
    userId: req.user?.id ?? null,
    error: error instanceof Error ? error.message : String(error),
    stack: error instanceof Error ? error.stack ?? null : null,
  });
}

export async function getMyProfileHandler(req: Request, res: Response) {
  try {
    const userId = req.user?.id;

    if (!userId) {
      return res.status(401).json({
        message: 'Unauthorized',
      });
    }

    const profile = await getMyProfile(String(userId));

    console.info('[PROFILE_LOAD_OK]', {
      route: `${req.method} ${req.originalUrl}`,
      userId,
    });

    return res.status(200).json(profile);
  } catch (error) {
    logProfileError('[PROFILE_LOAD_ERROR]', req, error);
    return sendInternalServerError(res, error, 'Error loading my profile:');
  }
}

export async function updateMyProfileHandler(req: Request, res: Response) {
  try {
    const userId = req.user?.id;

    if (!userId) {
      return res.status(401).json({
        message: 'Unauthorized',
      });
    }

    const {
      lastName,
      dateOfBirth,
      nationality,
      maritalStatus,
      phone,
      street,
      zip,
      city,
      canton,
      hasCar,
      hasChildren,
      hasDog,
      childrenCount,
      healthInsuranceName,
      employerName,
    } = req.body ?? {};

    const updatedProfile = await updateMyProfile(String(userId), {
      lastName,
      dateOfBirth,
      nationality,
      maritalStatus,
      phone,
      street,
      zip,
      city,
      canton,
      hasCar,
      hasChildren,
      hasDog,
      childrenCount,
      healthInsuranceName,
      employerName,
    });

    // Nach erfolgreicher Profiländerung alle aktiven Umzugsfälle neu synchronisieren.
    await refreshActiveMoveCasesForUser(Number(userId));

    console.info('[PROFILE_UPDATE_OK]', {
      route: `${req.method} ${req.originalUrl}`,
      userId,
    });

    return res.status(200).json(updatedProfile);
  } catch (error) {
    logProfileError('[PROFILE_UPDATE_ERROR]', req, error);
    return sendInternalServerError(res, error, 'Error updating my profile:');
  }
}
