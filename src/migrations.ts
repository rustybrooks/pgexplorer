/*
import { sprintf } from 'sprintf-js';

import * as sql from './sql';

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

  execute(SQL: sql.SQLBase, dryRun = false, logs: string[] = null) {
    if (this.message) {
      this.log(logs, '%s', [this.message]);
    }

    try {
      this.log(logs, 'SQL Execute: %s', [this.statement]);
      SQL.execute(this.statement, null, dryRun, false);
    } catch (e) {
      this.log(logs, 'Error while running statment: %r', e);
      if (!this.ignoreError) {
        throw e;
      }
    }
  }
}

const registry = {};
export class Migration {
  version = null;

  message = null;

  logs = null;

  statements = null;

  constructor(version, message) {
    registry[version] = this;
    this.version = version;
    this.message = message;
    this.statements = [];
    this.logs = [];
  }

  log(logs, msg, args = null) {
    const formatted = sprintf(msg, ...args);
    logs.push(formatted);
    console.log(formatted);
  }

  async migrate(SQL: sql.SQLBase, dryRun = false, initial = false, apply_versions = null) {
    const logs = [];

    await SQL.execute(`
        create table if not exists migrations
        (
            migration_id
            serial
            primary
            key,
            migration_datetime
            timestamp,
            version_pre
            int,
            version_post
            int
        )`,
    );

    const res = await SQL.selectOne('select max(versionPost) as version from migrations');
    const { version } = res;
    let todo = Object.keys(registry).filter(x => x > version);
    todo.push(...apply_versions || []);
    todo = todo.sort();
    this.log(logs, `Version = ${version}, todo = ${todo}, initial=${initial}`);

    const versionPre = version;
    let versionPost = version;

    for (const v of todo) {
      this.log(logs, 'Running migration %d: %s', [v, registry[v].message]);
      for (const statement of registry[v].statements) {
        statement.execute(SQL, dryRun, logs);
      }

      if (v > versionPre) {
        versionPost = v;
      }
    }

    if (todo.length && !dryRun) {
      SQL.insert(
        'migrations',
        {
          migration_datetime: 'datetime.datetime.utcnow()',
          versionPre: version,
          versionPost,
        },
      );
    }

    return logs;
  }

  addStatement(statement: string, ignoreError: boolean, message: string) {
    this.statements.push(new MigrationStatement(statement, message, ignoreError));
  }
}
*/
