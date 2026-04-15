declare module 'pg' {
  export interface QueryResult<T = unknown> {
    rows: T[];
    rowCount: number | null;
  }

  export interface PoolClient {
    query<T = unknown>(
      text: string,
      values?: unknown[]
    ): Promise<QueryResult<T>>;
    release(): void;
  }

  export interface PoolConfig {
    connectionString?: string;
  }

  export class Pool {
    constructor(config?: PoolConfig);
    query<T = unknown>(
      text: string,
      values?: unknown[]
    ): Promise<QueryResult<T>>;
    connect(): Promise<PoolClient>;
    on(event: 'error', listener: (err: Error) => void): this;
  }
}
