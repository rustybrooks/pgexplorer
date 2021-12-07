#!/usr/bin/env ts-node

import dotenv from 'dotenv';
// import chalk from 'chalk';
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';

import * as db from '../src/db';

const setupDbMiddleware = (argv) => {
  // eslint-disable-next-line @typescript-eslint/dot-notation
  dotenv.config({ path: argv['env'] });
  db.setupDb();
};

async function cmdList(options) {
  if (['tables', 'table'].includes(options.type)) {
    const tables = await db.tables();
    console.table(tables);
  }
}

async function cmdDump(options) {
  const data = await db.dumpTable({ table: options.table });
  console.table(data);
}

async function main() {
  // const res = await db.tableConstraints();
  const res = await db.tableConstraintDeleteOrder({});
  console.table(res);
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
  handler: (options) =>
      cmdList(options).then(() => process.exit()),
});

yarg.command({
  command: 'dump <table>',
  handler: (options) =>
    cmdDump(options).then(() => process.exit()),
});

// Add normalizeCredentials to yargs
yargs.middleware(setupDbMiddleware);
yargs.parse();
// main().then(() => process.exit(0));
