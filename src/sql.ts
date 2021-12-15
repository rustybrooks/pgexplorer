import * as pg from 'pg';
import { URL } from 'url';

const sqlObjects = {};

interface SQLBaseParams {
  writeUrl: string;
  readUrls?: string[];
  poolSize?: number;
  writerIsReader?: boolean;
  sqlKey?: string;
}

export class SQLBase {
  pool = null;

  writeUrl = null;

  constructor({ writeUrl, poolSize = 5 }: SQLBaseParams) {
    const params = new URL(writeUrl);

    const config = {
      user: params.username.replace('%40', '@'),
      password: params.password,
      host: params.hostname,
      port: params.port,
      database: params.pathname.split('/')[1],
      ssl: params.protocol === 'https:',
      max: poolSize || 5,
      idleTimeoutMillis: 600 * 1000, // close idle clients after 1 second
      // connectionTimeoutMillis: 1000, // return an error after 1 second if connection could not be established
      maxUses: 1000, // close (and replace) a connection after it has been used 7500 times (see below for discussion)
    };
    this.pool = new pg.Pool(config);
    this.writeUrl = writeUrl;
  }

  whereClause(clauseList: string | string[], joinWith = 'and', prefix = 'where'): string {
    if (!clauseList) {
      return '';
    }

    const clause2 = typeof clauseList === 'string' ? [clauseList] : clauseList;
    const joinWith2 = ` ${joinWith.trim()} `;
    return `${prefix ? `${prefix} ` : ''}${clause2.join(joinWith2)}`;
  }

  inClause(inList: any[], offset = 0) {
    return inList.map((el, i) => `$${offset + i + 1}`).join(',');
  }

  orderBy(sortKey: string | string[]) {
    if (!sortKey) {
      return '';
    }
    const sortList = typeof sortKey === 'string' ? sortKey.split(',') : sortKey;
    const orderbyList = sortList.map(k => {
      if (k[0] === '-') {
        return `${k.slice(1)} desc`;
      }
      return k;
    });
    return `${orderbyList ? 'order by ' : ''}${orderbyList.join(', ')}`;
  }

  limit(page = null, limit = null) {
    if (page === null || limit === null) {
      return '';
    }

    return `offset ${(page - 1) * limit} limit ${limit}`;
  }

  async insert(tableName, data, batch_size = 200, on_duplicate = null, returning = false) {
    if (Array.isArray(data)) {
      console.log('batch size not handled', batch_size);
    }
    if (!data) {
      return 0;
    }
    const sample = Array.isArray(data) ? data[0] : data;
    const columns = Object.keys(sample).sort();
    const values = columns.map((c, i) => `$${i}`);

    const query = `
        insert into ${tableName}(${columns.join(', ')})
        values (${values.join(', ')}) 
        ${on_duplicate || ''} ${returning ? 'returning *' : ''}
    `;

    const client = await this.pool.connect();
    console.log('insert', query, data);
    return (await client.query(query, data)).rows; // what to return?
  }

  async update(tableName, where, data = null, whereData = null) {
    const bindvars = { ...data, ...whereData };
    const bindnames = Object.keys(bindvars);
    const bindMap = Object.fromEntries(bindnames.map((c, i) => [c, i]));
    const setValues = Object.keys(data).map(c => `$c=$${bindMap[c]}`);
    const query = `
        update ${tableName} set ${setValues.join(', ')}
        ${this.whereClause(where)}
    `;
    const client = await this.pool.connect();
    return (
      await client.query(
        query,
        bindnames.map(k => bindvars[k]),
      )
    ).rows;
  }

  async delete(tableName, where, data = null) {
    const query = `delete from ${tableName} ${this.whereClause(where)}`;
    const client = await this.pool.connect();
    return (await client.query(query, data || [])).rows;
  }

  async execute(query, data: any[] = null, dryRun = false, log = false) {
    if (dryRun || log) {
      console.log(`SQL Run: ${query}`);
    }
    if (dryRun) return null;

    const client = await this.pool.connect();
    return (await client.query(query, data || [])).rows;
  }

  async select(query, bindvars = []) {
    const client = await this.pool.connect();
    try {
      return (await client.query(query, bindvars)).rows;
    } finally {
      client.release();
    }
  }

  async selectOne(query, bindvars = [], allowZero = false) {
    const client = await this.pool.connect();
    try {
      const res = await client.query(query, bindvars);
      if (res.rows.length > 1 || (!allowZero && res.rows.length === 0)) {
        throw new Error(`Expected ${allowZero ? 'zero or one rows' : 'exactly one row'}, got ${res.rows.length}`);
      }
      return res.rows[0] || [];
    } finally {
      client.release();
    }
  }

  async selectZeroOrOne(query, bindvars = []) {
    return this.selectOne(query, bindvars, true);
  }
}

export function sqlFactory({ writeUrl, poolSize = 5, sqlKey }: SQLBaseParams) {
  if (!(sqlKey in sqlObjects)) {
    sqlObjects[sqlKey] = new SQLBase({
      writeUrl,
      poolSize,
    });
  }

  return sqlObjects[sqlKey];
}
