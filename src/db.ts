import dotenv from 'dotenv';
import * as fs from 'fs';
import { sqlFactory } from './sql';

// eslint-disable-next-line import/no-mutable-exports
export let SQL: any = null;

export enum TableConstraint {
  'all',
  'foreign',
  'check',
  'unique',
}

export enum TableClass {
  'table',
  'index',
  'sequence',
  'view',
  'materializedView',
  'compositeType',
  'toastTable',
  'foreignTable',
}

export const constraintMap: { [key in TableConstraint]: string } = {
  [TableConstraint.all]: 'a',
  [TableConstraint.foreign]: 'f',
  [TableConstraint.check]: 'c',
  [TableConstraint.unique]: 'u',
};

export const tableClassMap: { [key in TableClass]: string } = {
  [TableClass.table]: 'r',
  [TableClass.index]: 'i',
  [TableClass.sequence]: 'S',
  [TableClass.view]: 'v',
  [TableClass.materializedView]: 'm',
  [TableClass.compositeType]: 'c',
  [TableClass.toastTable]: 't',
  [TableClass.foreignTable]: 'f',
};

export const tableClassMapReversed: { [key: string]: string } = Object.fromEntries(Object.entries(tableClassMap).map(k => [k[1], k[0]]));

export function envToDbUrl(envfile: string, { database = null }: { database?: string } = {}) {
  const econfig = dotenv.parse(fs.readFileSync(envfile));
  const protocol = econfig.PGSSL === 'true' ? 'https' : 'http';
  return `${protocol}://${econfig.PGUSER.replace('@', '%40')}:${econfig.PGPASSWORD}@${econfig.PGHOST}:${econfig.PGPORT || 5432}/${
    database || econfig.PGDATABASE
  }`;
}

export function setupDb(envfile: string = null, writeUrl: string = null, sqlKey = 'default') {
  const config = {
    sqlKey,
    writeUrl: writeUrl || envToDbUrl(envfile),
  };
  SQL = sqlFactory(config);
  return SQL;
}

const attributeMap: { [id: string]: any } = {};
export async function lookupAttribute(tableId: number, attributeKey: string) {
  const attrKey = `${tableId}:${attributeKey}`;
  if (!(attrKey in attributeMap)) {
    const query = `
        select *
        from pg_attribute
        where attrelid = $1
          and attnum = $2
    `;
    attributeMap[attrKey] = await SQL.selectOne(query, [tableId, attributeKey]);
  }
  return attributeMap[attrKey];
}

export async function tables({
  schema = 'public',
  columns = null,
  sort = 'table_name',
  page = null,
  limit = null,
}: {
  schema?: string;
  columns?: string[];
  sort?: string | string[];
  page?: number;
  limit?: number;
} = {}) {
  const where = ['n.nspname = $1', `c.relkind='${tableClassMap[TableClass.table]}'`];
  const bindvars = [schema];

  if (columns) {
    where.push(`c.oid in (select attrelid from pg_attribute a where a.attname in (${SQL.inClause(columns, 1)}))`);
    bindvars.push(...columns);
  }

  const query = `
    select relname as table_name, nspname as schema_name, pg_relation_size('"' || relname || '"')::real as table_size
    from pg_catalog.pg_namespace n
    join pg_catalog.pg_class c on (n.oid=c.relnamespace)
    ${SQL.whereClause(where)}
    ${SQL.orderBy(sort)}
    ${SQL.limit(page, limit)}
  `;
  return SQL.select(query, bindvars);
}

export async function tableConstraints({
  table = null,
  schema = 'public',
  constraintTypes = TableConstraint.all,
  sort = null,
}: {
  table?: string;
  schema?: string;
  constraintTypes?: TableConstraint | TableConstraint[];
  sort?: string | string[];
} = {}) {
  const where = ['nsp.nspname = $1'];
  const bindvars = [schema];
  if (table) {
    where.push('rel.relname = $2');
    bindvars.push(table);
  }

  const theseConstraints: TableConstraint[] = constraintTypes instanceof Array ? constraintTypes : [constraintTypes];
  if (!theseConstraints.includes(TableConstraint.all)) {
    where.push(`contype in (${theseConstraints.map(c => `'${constraintMap[c]}'`)})`);
  }

  const query = `
    select 
           rel.relname as constraint_table,
           con.conrelid as constraint_table_id,
           con.conname as constraint_name, 
           con.contype as constraint_type, 
           con.conkey as constraint_attribute_keys,
           con.confrelid as constraint_foreign_table_id,
           con.confkey as constraint_foreign_table_attribute_keys,
           relf.relname as constraint_foreign_table
    from pg_catalog.pg_constraint con
    join pg_catalog.pg_class rel ON rel.oid = con.conrelid
    left join pg_catalog.pg_class relf ON relf.oid = con.confrelid
    join pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
    ${SQL.whereClause(where)}
    ${SQL.orderBy(sort)}
  `;
  const constraints = await SQL.select(query, bindvars);

  return Promise.all(
    constraints.map(async (row: any) => {
      const trow = { ...row };
      trow.constraint_attribute_columns = await Promise.all(
        row.constraint_attribute_keys.map(async (a: any) => {
          const r = await lookupAttribute(row.constraint_table_id, a);
          return r.attname;
        }),
      );

      if (row.constraint_foreign_table_attribute_keys) {
        trow.constraint_foreign_table_attribute_columns = await Promise.all(
          row.constraint_foreign_table_attribute_keys.map(async (a: string) => {
            const r = await lookupAttribute(row.constraint_foreign_table_id, a);
            return r.attname;
          }),
        );
      }

      return trow;
    }),
  );
}

export async function tableConstraintDeleteOrder({ schema = 'public' }: { schema?: string } = {}) {
  const tbls = (await tables({ schema })).map((t: any) => t.table_name);
  let constraints: any = await tableConstraints({ schema, constraintTypes: TableConstraint.foreign });

  const out: string[] = [];
  while (constraints.length) {
    const ourconstraintMap = Object.fromEntries(constraints.map((c: any) => [c.constraint_foreign_table, c.constraint_table]));
    out.push(...tbls.filter((t: string) => !out.includes(t) && !(t in ourconstraintMap)));
    constraints = constraints.filter((c: any) => !out.includes(c.constraint_table));
  }

  out.push(...tbls.filter((t: string) => !out.includes(t)));
  return out;
}

// // change to use pg_tables?
// export async function tablesWithColumn({ column }: { column: string }) {
//   const query = `
//     select t.table_name
//     from information_schema.tables t
//     inner join information_schema.columns c using (table_name, table_schema)
//     where c.column_name = '${column}'
//     and t.table_schema not in ('information_schema', 'pg_catalog')
//     and t.table_type = 'BASE TABLE'
//     order by t.table_schema
//   `;
//   return (await SQL.select(query)).map(row => row.table_name);
// }

export async function indexFromTableColumns({ table, columns }: { table: string; columns: string[] }) {
  const query = `
    select
      t.relname as table_name,
      i.relname as index_name,
      array_agg(a.attname) as column_names
    from
      pg_class t,
      pg_class i,
      pg_index ix,
      pg_attribute a
    where
      t.oid = ix.indrelid
      and i.oid = ix.indexrelid
      and a.attrelid = t.oid
      and a.attnum = ANY(ix.indkey)
      and t.relkind = 'r'
      and t.relname='${table}'
    group by 1, 2
    having array_agg(a.attname)='{${columns.join(',')}}'
  `;
  return SQL.select(query);
}

export async function dumpTable({
  table,
  batchSize = 1000,
  sort = null,
  page = null,
  limit = null,
}: {
  table: string;
  batchSize?: number;
  sort?: string | string[];
  page?: number;
  limit?: number;
}) {
  const query = `
      select * 
      from "${table}"
      ${SQL.orderBy(sort)}
      ${SQL.limit(page, limit)}
  `;
  return SQL.selectGenerator(query, [], batchSize);
}

export async function dumpQuery({ query, bindvars = [], batchSize = 1000 }: { query: string; bindvars?: any[]; batchSize?: number }) {
  return SQL.selectGenerator(query, bindvars, batchSize);
}

export async function classColumns({
  schema = 'public',
  sort = null,
  page = null,
  limit = null,
}: {
  schema?: string;
  sort?: string | string[];
  page?: number;
  limit?: number;
}) {
  const where = ['n.nspname=$1', 'pg_catalog.pg_table_is_visible(c.oid)', "relkind not in ('i')"];
  const bindvars = [schema];
  const query = `
      select
          c.relname as class_name,
          a.attname as column_name,
          pg_catalog.format_type(a.atttypid, a.atttypmod) as column_type,
          relkind as class_type
      from pg_catalog.pg_namespace n
      join pg_catalog.pg_class c on (n.oid=c.relnamespace)
      join pg_catalog.pg_attribute a on (a.attrelid=c.oid)
      ${SQL.whereClause(where)}
      ${SQL.orderBy(sort)}
      ${SQL.limit(page, limit)}
    `;
  return SQL.select(query, bindvars);
}

export async function indexes({
  schema = 'public',
  sort = null,
  page = null,
  limit = null,
}: {
  schema?: string;
  sort?: string | string[];
  page?: number;
  limit?: number;
} = {}) {
  const where = ['n.nspname=$1', "c.relkind='i'"];
  const bindvars = [schema];
  const query = `
      select
          ct.relname as table_name,
          c.relname as index_name,
          array_agg(a.attname order by attnum) as column_names,
          indisunique as is_unique, 
          indisprimary as is_primary
      from pg_catalog.pg_namespace n
      join pg_catalog.pg_class c on (n.oid=c.relnamespace)
      join pg_catalog.pg_index i on (i.indexrelid=c.oid)
      join pg_catalog.pg_attribute a on (a.attrelid=c.oid)
      join pg_catalog.pg_class ct on (ct.oid=i.indrelid)
      ${SQL.whereClause(where)}
      group by table_name, index_name, indisunique, indisprimary
      ${SQL.orderBy(sort)}
      ${SQL.limit(page, limit)}
  `;
  return SQL.select(query, bindvars);
}

export async function structure() {
  const out: { [id: string]: any } = {};
  const tblColumns = await classColumns({ sort: ['class_type', 'class_name', 'attnum'] });
  tblColumns.forEach((row: any) => {
    const outKey: string = TableClass[tableClassMapReversed[row.class_type] as unknown as TableClass];
    // const outKey = row.class_type;
    if (!(outKey in out)) {
      out[outKey] = [];
    }
    out[outKey].push(row);
  });

  const constraints = await tableConstraints({ sort: ['constraint_table', 'constraint_name'] });
  out.constraint = await Promise.all(
    constraints.map(async (row: any) => ({
      constraint_table: row.constraint_table,
      constraint_name: row.constraint_name,
      constraint_type: row.constraint_type,
      constraint_attribute_columns: await Promise.all(
        row.constraint_attribute_keys.map(async (a: string) => (await lookupAttribute(row.constraint_table_id, a)).attname),
      ),
      constraint_foreign_table_attribute_columns: await Promise.all(
        (row.constraint_foreign_table_attribute_keys || []).map(
          async (a: string) => (await lookupAttribute(row.constraint_foreign_table_id, a)).attname,
        ),
      ),
    })),
  );

  out.index = await indexes({ sort: ['index_name'] });
  return out;
}

export async function findDuplicateRows({ table, columns }: { table: string; columns: string[] }) {
  const indexColumns = columns.map(c => `"${c}"`).join(',');

  const query = `
      select ${indexColumns}, count(*)
      from ${table}
      group by ${indexColumns}
      having count(*) > 1
      order by ${indexColumns}
    `;
  return SQL.select(query);
}

export async function findRelatedRows(
  table: string,
  columns: string[],
  referenceColumn: string,
  relatedTable: string,
  tableId: any,
  keepId: any,
): Promise<string> {
  let sqlData = '';

  const query = `
    select count(*)
    from ${relatedTable}
    where ${referenceColumn} = '${tableId}'
  `;
  const res = await SQL.select(query);

  if (res[0].count === '0') return '';

  console.log(`parent_id=${tableId} ref_table=${relatedTable} count=${res[0].count}`);
  if (keepId === tableId) {
    return sqlData;
  }

  sqlData += `delete from ${relatedTable} where ${referenceColumn} = '${tableId}';\n`;
  return sqlData;
}

export async function findRelatedRowsMany(
  table: string,
  columns: string[],
  referenceColumn: string,
  relatedTables: string[],
  tableId: any,
  keepId: any,
): Promise<string> {
  let sqlData = '';

  for (const t of relatedTables) {
    sqlData += await findRelatedRows(table, columns, referenceColumn, t, tableId, keepId);
  }

  return sqlData;
}

export async function findRowsByKeys(
  values: any,
  table: string,
  primaryKey: string,
  columns: string[],
  referenceColumn: string,
  relatedTables: string[],
): Promise<string> {
  let sqlData = '';
  console.log(columns.map(c => `${c}=${values[c]}`, `(count=${values.count})`));
  const clause = columns.map(c => {
    if (values[c] === null) {
      return `"${c}" is null`;
    }
    return `"${c}"='${values[c]}'`;
  });
  const query = `
    select *
    from ${table}
    where ${clause.join(' and ')}
    order by ${primaryKey}
  `;
  const res = await SQL.select(query);
  if (!res.length) return '';

  const keepId = res.at(-1)[primaryKey];

  if (referenceColumn) {
    sqlData += `-- update ${res.length - 1} ids to ${referenceColumn}=${keepId} (keys=${columns.map(c => `${c}=${values[c]}`)})\n`;
  }
  for (const row of res) {
    if (referenceColumn) {
      sqlData += await findRelatedRowsMany(table, columns, referenceColumn, relatedTables, row[primaryKey], keepId);
    }
    if (row[primaryKey] !== keepId) {
      sqlData += `delete from ${table} where ${primaryKey} = '${row[primaryKey]}';\n`;
    }
  }

  if (sqlData) {
    sqlData = `\n\nbegin transaction;\n${sqlData}commit;\n`;
  }

  return sqlData;
}
