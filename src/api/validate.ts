import Joi from 'joi';
import { HttpError } from './errors';

export const id = Joi.number().integer().positive();

export function validate<T>(schema: Joi.Schema<T>, input: unknown): T {
  const { value, error } = schema.validate(input, { abortEarly: false, stripUnknown: true });
  if (error) {
    throw new HttpError(400, 'validation failed', error.details.map((d) => d.message));
  }
  return value;
}
