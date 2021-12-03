#!/usr/bin/env ts-node

import dotenv from 'dotenv';
import chalk from 'chalk';
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';

import * as db from '../src/db';

async function main() {
  const res = await db.tableConstraints();
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

// eslint-disable-next-line @typescript-eslint/dot-notation
dotenv.config({ path: yarg.argv['env'] });

db.setupDb();
main().then(x => process.exit(0));
