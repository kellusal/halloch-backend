import { Router } from 'express';
import { requireAuth } from '../../middleware/requireAuth';
import {
    getMyProfileHandler,
    updateMyProfileHandler,
} from './profil.controller';

const router = Router();

router.get('/me', requireAuth, getMyProfileHandler);
router.put('/me', requireAuth, updateMyProfileHandler);

export default router;