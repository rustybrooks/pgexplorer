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
export async function lookupAttribute(tableId, attributeKey) {
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

export async function tables(schema = 'public') {
  const where = ['schemaname=$1'];
  const bindvars = [schema];
  const query = `
    select *
    from pg_catalog.pg_tables
    ${SQL.whereClause(where)}
  `;
  return SQL.select(query, bindvars);
}

export async function tableConstraints(table: string = null, schema = 'public') {
  const where = ['nsp.nspname = $1'];
  const bindvars = [schema];
  if (table) {
    where.push('rel.relname = $2');
    bindvars.push(table);
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
    constraints.rows.map(async row => {
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
