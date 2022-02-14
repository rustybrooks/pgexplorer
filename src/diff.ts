function difference(objA: { [id: string]: any }, objB: { [id: string]: any }) {
  return Object.fromEntries(Object.entries(objA).filter(k => !(k[0] in objB)));
}

export function diffIndexes(indexes1: string[], indexes2: string[]) {
  const idxKey = (i: any) => `${i.table_name}:${i.column_names}:unique=${i.is_unique}:primary=${i.is_primary}`;
  const index1Names = Object.fromEntries(indexes1.map(i => [idxKey(i), i]));
  const index2Names = Object.fromEntries(indexes2.map(i => [idxKey(i), i]));
  return [Object.keys(difference(index1Names, index2Names)), Object.keys(difference(index2Names, index1Names))];
}
