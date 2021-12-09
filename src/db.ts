import { sqlFactory } from './sql';

let SQL = null;

enum TableConstraint {
  'all',
  'foreign',
  'check',
  'unique',
}

enum TableClass {
  'table',
  'index',
  'sequence',
  'view',
  'materializedView',
  'compositeType',
  'toastTable',
  'foreignTable',
}

export const constraintMap = {
  [TableConstraint.foreign]: 'f',
  [TableConstraint.check]: 'c',
  [TableConstraint.unique]: 'u',
};

export const tableClassMap = {
  [TableClass.table]: 'r',
  [TableClass.index]: 'i',
  [TableClass.sequence]: 'S',
  [TableClass.view]: 'v',
  [TableClass.materializedView]: 'm',
  [TableClass.compositeType]: 'c',
  [TableClass.table]: 't',
  [TableClass.foreignTable]: 'f',
};

export const tableClassMapReversed = Object.fromEntries(
  Object.entries(tableClassMap).map((k, v) => [v, k]),
);

export function setupDb() {
  const sqlKey = 'main';
  const protocol = 'http';
  const writeUrl = `${protocol}://${process.env.PGUSER.replace('%40', '@')}:${process.env.PGPASSWORD}@${process.env.PGHOST}:${process.env.PGPORT}/${process.env.PGDATABASE}`;
  const config = {
    sqlKey,
    writeUrl,
  };
  SQL = sqlFactory(config);
  return SQL;
}

const attributeMap = {};
async function lookupAttribute(tableId, attributeKey) {
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

export async function tables({ schema = 'public', sort = 'table_name' }: { schema?: string, sort?: string | string[] } = {}) {
  const where = ['schemaname=$1'];
  const bindvars = [schema];
  const query = `
    select tablename as table_name, hasindexes, hasrules, hastriggers, schemaname as schema_name
    from pg_catalog.pg_tables
    ${SQL.whereClause(where)}
    ${SQL.orderBy(sort)}
  `;
  console.log(query);
  return SQL.select(query, bindvars);
}

export async function tableConstraints({
  table = null,
  schema = 'public',
  constraintTypes = TableConstraint.all,
}: {
  table?: string;
  schema?: string;
  constraintTypes?: TableConstraint | TableConstraint[];
}) {
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
  `;
  const constraints = await SQL.select(query, bindvars);

  return Promise.all(
    constraints.map(async row => {
      const trow = { ...row };
      trow.constraint_attribute_columns = await Promise.all(
        row.constraint_attribute_keys.map(async a => {
          const r = await lookupAttribute(row.constraint_table_id, a);
          return r.attname;
        }),
      );

      if (row.constraint_foreign_table_attribute_keys) {
        trow.constraint_foreign_table_attribute_columns = await Promise.all(
          row.constraint_foreign_table_attribute_keys.map(async a => {
            const r = await lookupAttribute(row.constraint_foreign_table_id, a);
            return r.attname;
          }),
        );
      }

      return trow;
    }),
  );
}

export async function tableConstraintDeleteOrder({ schema = 'public' }: { schema?: string }) {
  const tbls = (await tables({ schema })).map(t => t.table_name);
  let constraints = await tableConstraints({ schema, constraintTypes: TableConstraint.foreign });

  const out = [];
  while (constraints.length) {
    const constraintMap = Object.fromEntries(constraints.map(c => [c.constraint_foreign_table, c.constraint_table]));
    out.push(...tbls.filter(t => !out.includes(t) && !(t in constraintMap)));
    constraints = constraints.filter(c => !out.includes(c.constraint_table));
  }

  out.push(...tbls.filter(t => !out.includes(t)));
  return out;
}

// change to use pg_tables?
export async function tablesWithColumn({ column }: { column: string }) {
  const query = `
    select t.table_name
    from information_schema.tables t
    inner join information_schema.columns c using (table_name, table_schema)
    where c.column_name = '${column}'
    and t.table_schema not in ('information_schema', 'pg_catalog')
    and t.table_type = 'BASE TABLE'
    order by t.table_schema
  `;
  return (await SQL.select(query)).map(row => row.table_name);
}

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
 table, sort = null, page = null, limit = null,
} : { table: string, sort?: string | string[], page?: number, limit?: number }) {
  const query = `
      select * 
      from "${table}"
      ${SQL.orderBy(sort)}
      ${SQL.limit(page, limit)}
  `;
  return SQL.select(query);
}

export async function classColumns({
 schema = 'public', sort = null, page = null, limit = null,
} : { schema?: string, sort?: string | string[], page?: number, limit?: number }) {
  const where = [
    'n.nspname=$1',
    'pg_catalog.pg_table_is_visible(c.oid)',
  ];
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
