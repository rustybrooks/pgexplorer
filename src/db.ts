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
export async function lookupAttribute(tableName, attributeKey) {
  const attrKey = `${tableName}:${attributeKey}`;
  console.log(attrKey);
  if (!(attrKey in attributeMap)) {
    const query = `
        select *
        from pg_attribute
        where rename = $1
          and attkey = $2
    `;
    const res = await SQL.selectOne(query, [tableName, attributeKey]);
    console.log(attrKey, res);
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
    SELECT 
           rel.relname as constraint_table,
           conname as constraint_name, 
           contype as constraint_type, 
           conkey as constraint_attribute_keys
    FROM pg_catalog.pg_constraint con
    INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
    INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
    ${SQL.whereClause(where)}
  `;
  const constraints = await SQL.select(query, bindvars);

  const out = [];
  constraints.rows.forEach(row => {
    const trow = { ...row };
    trow.attribute_constraint_columns = Promise.all(
      row.constraint_attribute_keys.map(async a => {
        const r = await lookupAttribute(row.constraint_table, a);
        console.log('after await', r);
        return r.conname;
      }),
    );
    console.log('trow', trow.attribute_constraint_columns);
    out.push(trow);
  });
  return out;
}
