import * as pg from 'pg';
// import pgspice from 'pg-spice';

// import { Pool } from 'pg';
import { URL } from 'url';

// pgspice.patch(pg);

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

  constructor(paramsx: SQLBaseParams) {
    let { writeUrl, readUrls, poolSize, writerIsReader } = paramsx;
    readUrls = readUrls || [];
    if (writerIsReader) {
      readUrls.push(writeUrl);
    }
    poolSize = poolSize || 5;
    writeUrl = writeUrl || '';
    writerIsReader = writerIsReader || false;

    const params = new URL(writeUrl);

    const config = {
      user: params.username,
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
  }

  whereClause(clauseList: string | string[], joinWith = 'and', prefix = 'where'): string {
    if (!clauseList) {
      return '';
    }

    const clause2 = typeof clauseList === 'string' ? [clauseList] : clauseList;
    const joinWith2 = ` ${joinWith.trim()} `;
    return `${prefix ? `${prefix} ` : ''}${clause2.join(joinWith2)}`;
  }

  inClause(inList: any[]) {
    return inList.map((el, i) => `\$${i + 1}`).join(',');
  }

  async select(query, bindvars) {
    const client = await this.pool.connect();
    try {
      return client.query(query, bindvars);
    } finally {
      client.release();
    }
  }

  async selectOne(query, bindvars) {
    const client = await this.pool.connect();
    try {
      const res = await client.query(query, bindvars);
      if (res.rows.length !== 1) {
        throw new Error('Expected exactly 1 row, got res.rows.length');
      }
      return res.rows[0];
    } finally {
      client.release();
    }
  }
}

export function sqlFactory(params: SQLBaseParams) {
  if (!(params.sqlKey in sqlObjects)) {
    //    writeUrl = writeUrl.formatUnicorn({database, password})
    //
    //    readUrls = readUrls.map(u => u.formatUnicorn({database, password}))
    sqlObjects[params.sqlKey] = new SQLBase(params);
  }

  return sqlObjects[params.sqlKey];
}
