// import * as pg from 'pg';
import pgPromise from 'pg-promise';
import { IMain } from 'pg-promise';
import { URL } from 'url';
import Cursor from 'pg-cursor';

const pgp: IMain = pgPromise({
  // query(e: any) {
  //   console.log('QUERY RESULT:', e.query);
  // },
  // receive(data: any, result: any, e: any) {
  //   console.log(`DATA FROM QUERY ${e.query} WAS RECEIVED.`);
  // },
});

const sqlObjects = {};

interface SQLBaseParams {
  writeUrl: string;
  readUrls?: string[];
  poolSize?: number;
  writerIsReader?: boolean;
  sqlKey?: string;
  password?: string;
  database?: string;
}

export class SQLBase {
  db = null;

  writeUrl = null;

  constructor({ writeUrl, poolSize = 5 }: SQLBaseParams) {
    const params = new URL(writeUrl);

    const config = {
      user: params.username.replace('%40', '@'),
      password: params.password,
      host: params.hostname,
      port: parseInt(params.port, 10),
      database: params.pathname.split('/')[1],
      ssl: params.protocol === 'https:',
      max: poolSize || 5,
      idleTimeoutMillis: 600 * 1000, // close idle clients after 1 second
      // connectionTimeoutMillis: 1000, // return an error after 1 second if connection could not be established
      maxUses: 1000, // close (and replace) a connection after it has been used 7500 times (see below for discussion)
    };
    this.db = pgp(config);
    this.writeUrl = writeUrl;
  }

  shutdown() {
    this.db.$pool.end();
  }

  autoWhere(data, asList = false, first = 0) {
    const cols = Object.keys(data).filter(v => data[v] !== null && data[v] !== undefined);
    if (asList) {
      const bindvars = cols.map(k => data[k]);
      return [cols.map((k, i) => `${k}=$${first + i + 1}`), bindvars];
    }
    return [cols.map(k => `${k}=$(${k})`), Object.fromEntries(cols.map(c => [c, data[c]]))];
  }

  whereClause(clauseList: string | string[], joinWith = 'and', prefix = 'where'): string {
    if (!clauseList.length) {
      return '';
    }

    const clause2 = typeof clauseList === 'string' ? [clauseList] : clauseList;
    const joinWith2 = ` ${joinWith.trim()} `;
    return `${prefix ? `${prefix} ` : ''}${clause2.join(joinWith2)}`;
  }

  inClause(inList: any[], offset = 0) {
    return inList.map((el, i) => `$${offset + i + 1}`).join(',');
  }

  orderBy(...sortKey) {
    const sortList = sortKey.length === 1 && typeof sortKey[0] === 'string' ? sortKey[0].split(',') : sortKey;

    const orderbyList = sortList
      .filter(k => k)
      .map(k => {
        if (k[0] === '-') {
          return `${k.slice(1)} desc`;
        }
        return `${k} asc`;
      });
    if (!orderbyList.length) {
      return '';
    }
    return `${orderbyList ? 'order by ' : ''}${orderbyList.join(', ')}`;
  }

  limit(page = null, limit = null) {
    if (!page || !limit) {
      return '';
    }

    return `${page > 1 ? `offset ${(page - 1) * limit} ` : ''}limit ${limit}`;
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
    const values = columns.map((c, i) => `$${i + 1}`);

    const query = `
        insert into ${tableName}(${columns.join(', ')})
        values (${values.join(', ')}) 
        ${on_duplicate || ''} ${returning ? 'returning *' : ''}
    `;

    const bindvars = columns.map(c => data[c]);
    return this.db.query(query, bindvars);
  }

  async update(tableName, where, whereData = null, data = null) {
    const bindvars = { ...data, ...whereData };
    const bindnames = Object.keys(bindvars);
    const bindMap = Object.fromEntries(bindnames.map((c, i) => [c, i]));
    const setValues = Object.keys(data).map(c => `${c}=$${bindMap[c] + 1}`);
    const query = `
        update ${tableName} set ${setValues.join(', ')}
        ${this.whereClause(where)}
    `;
    return this.db.query(
      query,
      bindnames.map(k => bindvars[k]),
    );
  }

  async truncate(tableName) {
    return this.db.query(`truncate table ${tableName}`);
  }

  async delete(tableName, where, data = null) {
    const query = `delete from ${tableName} ${this.whereClause(where)}`;
    return this.db.query(query, data || []);
  }

  async execute(query, data: any[] = null, dryRun = false, log = false) {
    if (dryRun || log) {
      console.log(`SQL Run: ${query}`);
    }
    if (dryRun) return null;

    return this.db.query(query, data || []);
  }

  async select(query, bindvars = []) {
    return this.db.query(query, bindvars);
  }

  async *selectGenerator(query, bindvars = [], batchSize = 100) {
    const client = await this.db.$pool.connect();
    try {
      const cursor = await client.query(new Cursor(query, bindvars));
      while (true) {
        const rows = await cursor.read(batchSize);
        if (!rows.length) {
          break;
        }
        for (const row of rows) {
          yield row;
        }
      }
    } finally {
      client.release();
    }
  }

  async selectOne(query, bindvars = [], allowZero = false) {
    const res = await this.db.query(query, bindvars);
    if (res.length > 1 || (!allowZero && res.length === 0)) {
      throw new Error(`Expected ${allowZero ? 'zero or one rows' : 'exactly one row'}, got ${res.rows.length}`);
    }
    return res.length ? res[0] : null;
  }

  async selectZeroOrOne(query, bindvars = []) {
    return this.selectOne(query, bindvars, true);
  }

  async selectColumn(query, bindvars = []) {
    const rows = await this.select(query, bindvars);
    if (!rows.length) {
      return [];
    }
    const col = Object.keys(rows[0])[0];
    return rows.map(row => row[col]);
  }

  async selectColumns(query, bindvars = []) {
    const res = await this.db.query(query, bindvars);
    const cols = res.length ? Object.keys(res[0]) : [];
    const out = Object.fromEntries(cols.map(c => [c, []]));
    res.forEach(row => {
      cols.forEach(c => out[c].push(row[c]));
    });
    return out;
  }
}

export function sqlFactory({ writeUrl, poolSize = 5, sqlKey = 'default' }: SQLBaseParams) {
  if (!(sqlKey in sqlObjects)) {
    sqlObjects[sqlKey] = new SQLBase({
      writeUrl,
      poolSize,
    });
  }

  return sqlObjects[sqlKey];
}
