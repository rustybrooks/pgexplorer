#!/usr/bin/env ts-node
import chalk from 'chalk';

import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import * as fs from 'fs';
import * as db from '../src/db';
import * as diff from '../src/diff';

const setupDbMiddleware = argv => {
  // eslint-disable-next-line @typescript-eslint/dot-notation
  db.setupDb(argv['env'], argv['env']);
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
  const out = await db.structure();
  fs.writeFileSync(options.output, JSON.stringify(out, null, 2));
}

async function cmdCompare(options) {
  const db1 = await db.structure();
  let db2;

  if (options.env2) {
    db.setupDb(options.env2, options.env2);
    db2 = await db.structure();
  } else if (options.structure) {
    db2 = JSON.parse(fs.readFileSync(options.structure, 'utf8'));
  }

  // console.log(jsondiff.diffString(db1, db2));
  console.log(chalk.magenta('======== indexes'));
  const [i1, i2] = diff.diffIndexes(db1.index, db2.index);
  i1.forEach(i => console.log('-', chalk.red(i)));
  i2.forEach(i => console.log('+', chalk.green(i)));
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

yarg.command({
  command: 'compare',
  builder: y => {
    y.option('env2', { describe: 'Env file for 2nd database', default: null });
    y.option('structure', { describe: 'output file from previous structure run', default: 'structure.json' });
    return y;
  },
  handler: options => cmdCompare(options).then(() => process.exit()),
});

// Add normalizeCredentials to yargs
yargs.middleware(setupDbMiddleware);
yargs.parse();
// main().then(() => process.exit(0));
