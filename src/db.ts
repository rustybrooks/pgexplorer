import { sqlFactory } from './sql';

let SQL = null;

export function setupDb() {
  const sqlKey = 'main';
  const writeUrl = `http://${process.env.PGUSER}:${process.env.PGPASSWORD}@${process.env.PGHOST}:${process.env.PGPORT}/${process.env.PGDATABASE}`;
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
    const res = await SQL.selectOne(query, [tableId, attributeKey]);
    attributeMap[attrKey] = res;
  }
  return attributeMap[attrKey];
}

export async function tables({ schema = 'public' }: { schema?: string }) {
  const where = ['schemaname=$1'];
  const bindvars = [schema];
  const query = `
    select *
    from pg_catalog.pg_tables
    ${SQL.whereClause(where)}
  `;
  return SQL.select(query, bindvars);
}

enum TableConstraint {
  'all',
  'foreign',
  'check',
  'unique',
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

  const constraintMap = {
    [TableConstraint.foreign]: 'f',
    [TableConstraint.check]: 'c',
    [TableConstraint.unique]: 'u',
  };

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
  const tbls = (await tables({ schema })).map(t => t.tablename);
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
