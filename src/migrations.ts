import { sprintf } from 'sprintf-js';

import * as pgexplorer from '.';

export class MigrationStatement {
  statement: string = null;

  message: string = null;

  ignoreError = false;

  constructor(statement: string, message: string = null, ignoreError = false) {
    this.statement = statement;
    this.message = message;
    this.ignoreError = ignoreError;
  }

  log(logs: string[], msg: string, args: any[]) {
    const formatted = sprintf(msg, ...args);
    if (logs) {
      logs.push(formatted);
    }

    console.log(formatted);
  }

  async execute(SQL: pgexplorer.sql.SQLBase, dryRun = false, logs: string[] = null) {
    if (this.message) {
      this.log(logs, '%s', [this.message]);
    }

    try {
      this.log(logs, 'SQL Execute: %s', [this.statement]);
      await SQL.execute(this.statement, null, dryRun, false);
    } catch (e) {
      this.log(logs, 'Error while running statment: %r', e);
      if (!this.ignoreError) {
        throw e;
      }
    }
  }
}

export class Migration {
  static registry: { [id: string]: Migration } = {};

  version: number = null;

  message: string = null;

  logs: string[] = null;

  statements: MigrationStatement[] = null;

  constructor(version: number, message: string) {
    Migration.registry[version] = this;
    this.version = version;
    this.message = message;
    this.statements = [];
    this.logs = [];
  }

  static log(logs: string[], msg: string, args: any[] = null) {
    const formatted = sprintf(msg, ...(args || []));
    logs.push(formatted);
    console.log(formatted);
  }

  static async migrate(SQL: pgexplorer.sql.SQLBase, initial = false, applyVersions: number[] = null, dryRun = false) {
    const logs: string[] = [];

    await SQL.execute(`
        create table if not exists migrations (
            migration_id serial primary key,
            migration_datetime timestamp,
            version_pre int,
            version_post int
        )
    `);

    const res = await SQL.selectOne('select max(version_post) as version from migrations');
    const { version } = res;
    let todo = Object.keys(Migration.registry)
      .map(x => parseInt(x, 10))
      .filter(x => initial || x > version);
    todo.push(...(applyVersions || []));
    todo = todo.sort();
    this.log(logs, `Version = ${version}, todo = ${todo}, initial=${initial}`);

    const versionPre = version;
    let versionPost = version;

    for (const v of todo) {
      Migration.log(logs, 'Running migration %d: %s', [v, Migration.registry[v].message]);
      for (const statement of Migration.registry[v].statements) {
        await statement.execute(SQL, dryRun, logs);
      }
      if (v > versionPre) {
        versionPost = v;
      }
    }

    if (todo.length && !dryRun) {
      await SQL.insert('migrations', {
        migration_datetime: new Date(),
        version_pre: version,
        version_post: versionPost,
      });
    }

    return logs;
  }

  addStatement(statement: string, ignoreError = false, message: string = null) {
    this.statements.push(new MigrationStatement(statement, message, ignoreError));
  }
}
