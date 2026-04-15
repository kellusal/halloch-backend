import { Router } from 'express';
import { requireAuth } from '../../middleware/requireAuth';
import * as authController from './auth.controller';

const authRouter = Router();

authRouter.post('/register', authController.register);
authRouter.post('/login', authController.login);
authRouter.get('/me', requireAuth, authController.me);

export default authRouter;
