import { Pool } from 'pg';
import { URL } from 'url';

const sqlObjects = {};

export class SQLBase {
  pool = null;

  constructor({ writeUrl, readUrls, poolSize, writerIsReader }) {
    let theseReadUrls = readUrls || [];
    if (writerIsReader) {
      theseReadUrls += writeUrl;
    }

    const params = new URL(writeUrl);

    const config = {
      user: params.username,
      password: params.password,
      host: params.hostname,
      port: params.port,
      database: params.pathname.split('/')[1],
      ssl: (params.protocol = 'https:'),
      max: poolSize,
      idleTimeoutMillis: 600 * 1000, // close idle clients after 1 second
      // connectionTimeoutMillis: 1000, // return an error after 1 second if connection could not be established
      maxUses: 1000, // close (and replace) a connection after it has been used 7500 times (see below for discussion)
    };

    this.pool = Pool(config);
  }
}

export function sqlFactory({ sqlKey, password, database, writeUrl, readUrls, poolSize, writerIsReader }) {
  if (!(sqlKey in sqlObjects)) {
    //    writeUrl = writeUrl.formatUnicorn({database, password})
    //
    //    readUrls = readUrls.map(u => u.formatUnicorn({database, password}))
    sqlObjects[sqlKey] = new SQLBase({
      writeUrl,
      readUrls,
      poolSize,
      writerIsReader,
    });
  }

  return sqlObjects[sqlKey];
}
