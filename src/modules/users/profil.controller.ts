import { Request, Response } from 'express';
import { sendInternalServerError } from '../../middleware/error.middleware';
import { refreshActiveMoveCasesForUser } from '../move/move.task-sync';
import { getMyProfile, updateMyProfile } from './profil.repository';

export async function getMyProfileHandler(req: Request, res: Response) {
  try {
    const userId = req.user?.id;

    if (!userId) {
      return res.status(401).json({
        message: 'Unauthorized',
      });
    }

    const profile = await getMyProfile(String(userId));

    return res.status(200).json(profile);
  } catch (error) {
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

    return res.status(200).json(updatedProfile);
  } catch (error) {
    return sendInternalServerError(res, error, 'Error updating my profile:');
  }
}