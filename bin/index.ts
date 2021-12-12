#!/usr/bin/env ts-node

import dotenv from 'dotenv';
import yargs, { describe } from 'yargs';
import { hideBin } from 'yargs/helpers';
// import * as JSON from 'JSON';
import * as fs from 'fs';
import * as db from '../src/db';

const setupDbMiddleware = argv => {
  // eslint-disable-next-line @typescript-eslint/dot-notation
  dotenv.config({ path: argv['env'] });
  db.setupDb();
};

async function cmdList(options) {
  if (['tables', 'table'].includes(options.type)) {
    const tables = await db.tables();
    console.table(tables);
  } else if (['index', 'indexes', 'indices'].includes(options.type)) {
    const indexes = await db.indexes();
    console.table(indexes);
  } else if (['constraints', 'constraint'].includes(options.type)) {
    const constraints = (await db.tableConstraints({ sort: ['constraint_table', 'constraint_name'] })).map(row => {
      const cols = ['constraint_table', 'constraint_name', 'constraint_type', 'constraint_attribute_columns'];
      return Object.fromEntries(cols.map(c => [c, row[c]]));
    });
    console.table(constraints);
  }
}

async function cmdDump(options) {
  const data = await db.dumpTable({ table: options.table });
  console.table(data);
}

async function cmdStructure(options) {
  const out: { [id: string]: any } = {};
  // const tables = await db.tables();
  const tblColumns = await db.classColumns({ sort: ['class_type', 'class_name', 'attnum'] });
  tblColumns.forEach(row => {
    const outKey: string = db.TableClass[db.tableClassMapReversed[row.class_type]];
    // const outKey = row.class_type;
    if (!(outKey in out)) {
      out[outKey] = [];
    }
    out[outKey].push(row);
  });

  const constraints = await db.tableConstraints({ sort: ['constraint_table', 'constraint_name'] });
  out.constraint = await Promise.all(
    constraints.map(async row => ({
      constraint_table: row.constraint_table,
      constraint_name: row.constraint_name,
      constraint_type: row.constraint_type,
      constraint_attribute_columns: await Promise.all(
        row.constraint_attribute_keys.map(async a => (await db.lookupAttribute(row.constraint_table_id, a)).attname),
      ),
      constraint_foreign_table_attribute_columns: await Promise.all(
        (row.constraint_foreign_table_attribute_keys || []).map(
          async a => (await db.lookupAttribute(row.constraint_foreign_table_id, a)).attname,
        ),
      ),
    })),
  );

  out.index = await db.indexes({ sort: ['class_name'] });

  // console.log();
  fs.writeFileSync(options.output, JSON.stringify(out, null, 2));
}

const yarg = yargs(hideBin(process.argv));

yarg.usage('Usage: <subcommand>');
yarg.wrap(Math.min(130, yarg.terminalWidth()));

yarg.option('env', {
  describe: 'name of env file to use for database creds',
  default: '.env.local',
  type: 'string',
});

yarg.command({
  command: 'list <type>',
  handler: options => cmdList(options).then(() => process.exit()),
});

yarg.command({
  command: 'dump <table>',
  handler: options => cmdDump(options).then(() => process.exit()),
});

yarg.command({
  command: 'structure',
  builder: y => {
    y.option('output', { describe: 'output file name', default: 'structure.json' });
    return y;
  },
  handler: options => cmdStructure(options).then(() => process.exit()),
});

// Add normalizeCredentials to yargs
yargs.middleware(setupDbMiddleware);
yargs.parse();
// main().then(() => process.exit(0));
