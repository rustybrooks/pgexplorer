#!/usr/bin/env ts-node
import chalk from 'chalk';
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import * as fs from 'fs';
import * as csv from 'csv-writer';
import * as path from 'path';
import * as db from '../src/db';
import * as diff from '../src/diff';

const setupDbMiddleware = argv => {
  // eslint-disable-next-line @typescript-eslint/dot-notation
  db.setupDb(argv['env'], null, argv['env']);
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
  let fh = null;

  const dumpRows = rows => {
    if (!rows.length) return;
    if (options.output) {
      if (fh === null) {
        fh = csv.createObjectCsvWriter({
          path: options.output,
          header: Object.keys(rows[0]).map(k => ({ id: k, title: k })),
        });
      }
      fh.writeRecords(rows);
      console.log(`wrote ${rows.length}`);
    } else {
      console.table(rows);
    }
  };

  const gen = options.table.split(' ') === 1 ? await db.dumpTable({ table: options.table }) : await db.dumpQuery({ query: options.table });

  let these = [];
  while (true) {
    const i = await gen.next();
    if (i.done) break;

    these.push(i.value);
    if (these.length === 1000) {
      dumpRows(these);
      these = [];
    }
  }
  dumpRows(these);
}

async function cmdStructure(options) {
  const out = await db.structure();
  fs.writeFileSync(options.output, JSON.stringify(out, null, 2));
}

async function cmdCompare(options) {
  const db1 = await db.structure();
  let db2;

  if (options.env2) {
    db.setupDb(options.env2, null, options.env2);
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

async function cmdCheckConstraints(options) {
  const config = JSON.parse(fs.readFileSync(options.config, 'utf8'));
  const { tableRefs, ignoreTables } = config;

  const tableMap = {};
  for (const key of Object.keys(tableRefs)) {
    tableMap[key] = [];
    for (const col of tableRefs[key]) {
      const tables = await db.tables({ columns: [col], sort: ['table_name'] });
      tableMap[key].push(
        ...tables
          .map(row => row.table_name)
          .filter(t => !ignoreTables.includes(t))
          .map(t => [t, col]),
      );
    }
  }

  if (options.table) {
    console.log('|parent|child|conflicts|percentage|');
  }

  for (const parentTable of Object.keys(tableMap)) {
    for (const childTable of tableMap[parentTable]) {
      const query = `
          select count(*) as count
          from ${childTable[0]} c
          left join ${parentTable} p on (c."${childTable[1]}" = p.id)
          where p.id is null
      `;
      // console.log(query);
      const count = await db.SQL.selectOne(query);
      const countAll = await db.SQL.selectOne(`select count(*) as count from ${childTable[0]}`);
      const countPct = (100.0 * count.count) / countAll.count;

      if (options.table) {
        const countStr = `${count.count} / ${countAll.count}|${countPct.toFixed(2)}%`;
        console.log(`|${parentTable}|${childTable}|${countStr}|`);
      } else {
        let countStr = `${count.count} / ${countAll.count} = ${countPct.toFixed(2)}%`;
        if (countPct < 0.0001) {
          countStr = chalk.green(countStr);
        } else if (countPct >= 10) {
          countStr = chalk.red(countStr);
        } else if (countPct >= 1) {
          countStr = chalk.yellow(countStr);
        }
        console.log(chalk.magenta(parentTable), '<-', chalk.cyan(`${childTable[0]}(${childTable[1]})`), ': ', countStr);
      }
    }
  }
}

export interface UniqueIndexType {
  table: string;
  columns: string[];
  reference_column: string | null;
}

export class SqlHandler {
  dirname!: string;

  file: any;

  constructor(dirname: string) {
    this.dirname = dirname;
    if (fs.existsSync(this.dirname)) {
      throw Error(`directory ${this.dirname} already exists, will not continue`);
    }
    fs.mkdirSync(this.dirname);
  }

  add(idx: UniqueIndexType, sqlData: string) {
    const fname = `${path.join(this.dirname, idx.table)}.sql`;
    if (!fs.existsSync(fname)) {
      fs.writeFileSync(fname, `-- begin dump ${idx.table}(${idx.columns.join(',')})\n\n`);
    }

    fs.appendFileSync(fname, sqlData);
  }
}

async function cmdCheckUnique(options: any) {
  const uniqueIndices = JSON.parse(fs.readFileSync(options.config, 'utf8'));

  const sql = new SqlHandler(options.outdir);

  for (const idx of uniqueIndices) {
    const relatedTableRows = idx.reference_column ? await db.tables({ columns: [idx.reference_column] }) : [];
    const relatedTables = relatedTableRows.map(x => x.table_name);

    const duplicateRows = await db.findDuplicateRows(idx);

    console.log(
      chalk.green('-----------------------------------', idx.table, idx.columns, `related=${relatedTables}`, duplicateRows.length),
    );

    for (const row of duplicateRows) {
      sql.add(idx, await db.findRowsByKeys(row, idx.table, idx.columns, idx.reference_column, relatedTables));
    }
  }
}

// ************************************

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
  builder: y => {
    y.option('output', { describe: 'output file name', default: 'dump.csv' });
    return y;
  },
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

yarg.command({
  command: 'check-constraints',
  builder: y => {
    y.option('config', { describe: 'config file describing new constraints', default: 'structure.json' });
    y.boolean('table');
    y.describe('table', 'Output in JIRA-compliant table markdown');
    y.default('table', false);
    return y;
  },
  handler: options => cmdCheckConstraints(options).then(() => process.exit()),
});

yarg.command({
  command: 'check-unique',
  builder: y => {
    y.option('config', { describe: 'config file describing new indexes', default: 'indexes.json' });
    y.option('outdir', { describe: 'directory to output SQL to (must not exist already)' });
    return y;
  },
  handler: options => cmdCheckUnique(options).then(() => process.exit()),
});

// Add normalizeCredentials to yargs
yargs.middleware(setupDbMiddleware);
yargs.parse();
// main().then(() => process.exit(0));
