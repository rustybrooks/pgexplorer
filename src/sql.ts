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
