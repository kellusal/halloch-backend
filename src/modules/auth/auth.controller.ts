import { Request, Response } from 'express';
import * as authService from './auth.service';

export async function register(req: Request, res: Response) {
  try {
    const result = await authService.register({
      email: req.body.email,
      password: req.body.password,
      firstName: req.body.firstName,
      lastName: req.body.lastName,
      language: req.body.language,
    });

    return res.status(201).json(result);
  } catch (error) {
    const message =
      error instanceof Error ? error.message : 'Registration failed';

    if (
      message === 'Email already exists' ||
      message === 'Email is required' ||
      message === 'Password is required' ||
      message === 'Password must be at least 6 characters long'
    ) {
      return res.status(400).json({ message });
    }

    return res.status(500).json({ message: 'Internal server error' });
  }
}

export async function login(req: Request, res: Response) {
  try {
    const result = await authService.login({
      email: req.body.email,
      password: req.body.password,
    });

    return res.status(200).json(result);
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Login failed';

    if (
      message === 'Email is required' ||
      message === 'Password is required'
    ) {
      return res.status(400).json({ message });
    }

    if (
      message === 'Invalid email or password' ||
      message === 'User is inactive'
    ) {
      return res.status(401).json({ message });
    }

    return res.status(500).json({ message: 'Internal server error' });
  }
}

export async function me(req: Request, res: Response) {
  try {
    if (!req.user?.id) {
      return res.status(401).json({ message: 'Unauthorized' });
    }

    const result = await authService.getMe(req.user.id);
    return res.status(200).json(result);
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Could not load user';

    if (message === 'User not found') {
      return res.status(404).json({ message });
    }

    return res.status(500).json({ message: 'Internal server error' });
  }
}