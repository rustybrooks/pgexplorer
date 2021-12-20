import * as sql from '../src/sql';

const SQL = sql.sqlFactory({
  writeUrl: 'http://wombat:1wombat2@localhost:5434/pgexplorer_test',
});

describe('Test SQL Basic', () => {
  beforeEach(async () => {
    await SQL.execute('drop table if exists foo');
    await SQL.execute('create table foo(bar integer, baz varchar(20))');
  });

  afterAll(async () => {
    await SQL.db.$pool.end();
  });

  it('test_select_column', async () => {
    await SQL.insert('foo', { bar: 1, baz: 'aaa' });
    await SQL.insert('foo', { bar: 2, baz: 'bbb' });
    expect(await SQL.selectColumn('select bar from foo order by bar')).toStrictEqual([1, 2]);
    expect(await SQL.selectColumn('select baz from foo order by bar')).toStrictEqual(['aaa', 'bbb']);
  });

  it('test_select_columns', async () => {
    await SQL.insert('foo', { bar: 1, baz: 'aaa' });
    await SQL.insert('foo', { bar: 2, baz: 'bbb' });
    expect(await SQL.selectColumns('select bar, baz from foo order by bar')).toStrictEqual({ bar: [1, 2], baz: ['aaa', 'bbb'] });
  });

  it('test_select', async () => {
    await SQL.insert('foo', { bar: 1, baz: 'aaa' });
    await SQL.insert('foo', { bar: 2, baz: 'bbb' });
    const fe = await SQL.select('select * from foo order by bar');
    expect(fe).toStrictEqual([
      { bar: 1, baz: 'aaa' },
      { bar: 2, baz: 'bbb' },
    ]);
  });

  it('test_selectGenerator', async () => {
    await SQL.insert('foo', { bar: 1, baz: 'aaa' });
    await SQL.insert('foo', { bar: 2, baz: 'bbb' });
    const fe = await SQL.selectGenerator('select * from foo order by bar');
    const feval = [];
    while (true) {
      const i = await fe.next();
      if (i.done) break;
      feval.push(i.value);
    }
    expect(feval).toStrictEqual([
      { bar: 1, baz: 'aaa' },
      { bar: 2, baz: 'bbb' },
    ]);
  });

  it('test_selectOne', async () => {
    await SQL.insert('foo', { bar: 1, baz: 'aaa' });
    await SQL.insert('foo', { bar: 2, baz: 'bbb' });
    await SQL.insert('foo', { bar: 2, baz: 'ccc' });
    expect(await SQL.selectOne('select * from foo where bar=1')).toStrictEqual({ bar: 1, baz: 'aaa' });
    await expect(async () => {
      await SQL.selectOne('select * from foo where bar=3');
    }).rejects.toThrow(Error);
    await expect(async () => {
      await SQL.selectOne('select * from foo where bar=2');
    }).rejects.toThrow(Error);
  });

  it('test_selectZeroOrOne', async () => {
    await SQL.insert('foo', { bar: 1, baz: 'aaa' });
    await SQL.insert('foo', { bar: 2, baz: 'bbb' });
    await SQL.insert('foo', { bar: 2, baz: 'ccc' });
    expect(await SQL.selectOne('select * from foo where bar=1')).toStrictEqual({ bar: 1, baz: 'aaa' });
    expect(await SQL.selectZeroOrOne('select * from foo where bar=3')).toBe(null);
    await expect(async () => {
      await SQL.selectZeroOrOne('select * from foo where bar=2');
    }).rejects.toThrow(Error);
  });

  it('test_update_delete', async () => {
    await SQL.insert('foo', { bar: 1, baz: 'aaa' });
    await SQL.insert('foo', { bar: 2, baz: 'bbb' });
    await SQL.insert('foo', { bar: 2, baz: 'ccc' });
    expect(await SQL.select('select * from foo order by bar')).toStrictEqual([
      { bar: 1, baz: 'aaa' },
      { bar: 2, baz: 'bbb' },
      { bar: 2, baz: 'ccc' },
    ]);

    await SQL.update('foo', 'bar=$1', [2], { baz: 'xxx' });
    expect(await SQL.select('select * from foo order by bar')).toStrictEqual([
      { bar: 1, baz: 'aaa' },
      { bar: 2, baz: 'xxx' },
      { bar: 2, baz: 'xxx' },
    ]);

    await SQL.delete('foo', 'bar=$1', [1]);
    expect(await SQL.select('select * from foo order by bar')).toStrictEqual([
      { bar: 2, baz: 'xxx' },
      { bar: 2, baz: 'xxx' },
    ]);
  });
});

describe('Test Helpers', () => {
  it('test_inClause', () => {
    const mylist = [1, 2, 3, 4, 5];
    const expected = '$1,$2,$3,$4,$5';
    expect(expected).toStrictEqual(SQL.inClause(mylist));
  });

  it('test_whereClause', () => {
    expect(SQL.whereClause([])).toStrictEqual('');
    expect(SQL.whereClause(['a=b'])).toStrictEqual('where a=b');
    expect(SQL.whereClause('a=b')).toStrictEqual('where a=b');
    expect(SQL.whereClause(['a=b'], 'and', 'and')).toStrictEqual('and a=b');
    expect(SQL.whereClause(['a=b'], 'and', '')).toStrictEqual('a=b');
    expect(SQL.whereClause(['a=b', 'b=c'])).toStrictEqual('where a=b and b=c');
    expect(SQL.whereClause(['a=b', 'b=c'], 'or')).toStrictEqual('where a=b or b=c');
  });

  it('test_autoWhere', async () => {
    let [w, b] = SQL.autoWhere({ a: 1, b: 2, c: 3 }, true);
    expect(w).toStrictEqual(['a=$1', 'b=$2', 'c=$3']);
    expect(b).toStrictEqual([1, 2, 3]);

    [w, b] = SQL.autoWhere({ a: 1, b: 2, c: 3 });
    expect(w).toStrictEqual(['a=$(a)', 'b=$(b)', 'c=$(c)']);
    expect(b).toStrictEqual({ a: 1, b: 2, c: 3 });

    [w, b] = SQL.autoWhere({
      a: 1,
      b: 2,
      c: 3,
      d: null,
      e: undefined,
      f: false,
    });
    expect(w).toStrictEqual(['a=$(a)', 'b=$(b)', 'c=$(c)', 'f=$(f)']);
    expect(b).toStrictEqual({
      a: 1,
      b: 2,
      c: 3,
      f: false,
    });
  });

  it('test_orderBy', async () => {
    expect(SQL.orderBy(null)).toBe('');
    expect(SQL.orderBy(null, 'foo')).toBe('order by foo asc');
    expect(SQL.orderBy(null, '-foo')).toBe('order by foo desc');
    expect(SQL.orderBy('-bar', 'foo')).toBe('order by bar desc, foo asc');
    expect(SQL.orderBy('-bar,foo')).toBe('order by bar desc, foo asc');
  });

  it('test_limit', async () => {
    expect(SQL.limit(0, 0)).toBe('');
    expect(SQL.limit(null, 0)).toBe('');
    expect(SQL.limit(null, 10)).toBe('');
    expect(SQL.limit(1, 10)).toBe('limit 10');
    expect(SQL.limit(2, 10)).toBe('offset 10 limit 10');
  });

  it('test_', async () => {});
  it('test_', async () => {});
  it('test_', async () => {});
  it('test_', async () => {});
});

/*

class TestTransactions(unittest.TestCase):

    def _test_as_readonly(self):
        SQL.insert("foo", {"bar": 1, "baz": "aaa"})
        SQL.insert("foo", {"bar": 2, "baz": "bbb"})

        with SQL.as_readonly():
            self.assertEquals(
                [1, 2], list(SQL.select_column("select bar from foo order by bar"))
            )

    def test_implied_readonly(self):
        SQLW = SQLBase(
            write_url="mysql+pymysql://wombat:{password}@unit_test-mysql/{database}?charset=utf8mb4",
            read_urls=[
                "mysql+pymysql://wombat:{password}@unit_test-mysql/{database}?charset=utf8mb4"
            ],
        )
        self.assertEqual(SQLW.get_readonly(), False)

        SQLW2 = SQLBase(
            write_url="mysql+pymysql://wombat:{password}@unit_test-mysql/{database}?charset=utf8mb4",
        )
        self.assertEqual(SQLW2.get_readonly(), False)

        SQLRO = SQLBase(
            write_url=null,
            read_urls=[
                "mysql+pymysql://wombat:{password}@unit_test-mysql/{database}?charset=utf8mb4"
            ],
        )
        self.assertEqual(SQLRO.get_readonly(), True)

    def test_result_count(self):
        SQL.insert("foo", {"bar": 1, "baz": "aaa"})
        SQL.insert("foo", {"bar": 2, "baz": "bbb"})
        r = list(SQL.select_foreach("select bar from foo"))
        count_query = "select count(*) as count from foo"
        self.assertEqual(
            SQL.result_count(False, r, count_query), [{"bar": 1}, {"bar": 2}]
        )
        self.assertEqual(
            SQL.result_count(True, r, count_query),
            {"results": [{"bar": 1}, {"bar": 2}], "count": 2},
        )

    @SQL.is_transaction
    def _myfn(self, data, data2=null, err=False, err2=False):
        for el in data:
            SQL.insert("foo", {"bar": el})

        if data2:
            self._myfn(data=data2, err=err2)

        if err:
            raise TestException("sup")

    def test_transaction_decorator_commit(self):
        self._myfn([1, 2])
        self.assertEquals([1, 2], list(SQL.select_column("select bar from foo")))

    def test_transaction_decorator_rollback(self):
        with self.assertRaises(TestException):
            self._myfn([1, 2], err=True)
        self.assertEquals([], list(SQL.select_column("select bar from foo")))

    def test_transaction_decorator_nested_commit(self):
        self._myfn([1, 2], data2=[3, 4])
        self.assertEquals([1, 2, 3, 4], list(SQL.select_column("select bar from foo")))

    def test_transaction_decorator_nested_rollback1(self):
        with self.assertRaises(TestException):
            self._myfn([1, 2], data2=[3, 4], err=True)
        self.assertEquals([], list(SQL.select_column("select bar from foo")))

    def test_transaction_decorator_nested_rollback2(self):
        with self.assertRaises(TestException):
            self._myfn([1, 2], data2=[3, 4], err2=True)
        self.assertEquals([], list(SQL.select_column("select bar from foo")))

    def test_basic_commit(self):
        # basic
        with SQL.transaction():
            SQL.insert("foo", {"bar": 1})
            SQL.insert("foo", {"bar": 2})

            # within the transaction we should see what we inserted
            self.assertEquals([1, 2], list(SQL.select_column("select bar from foo")))

        # after/outside the transaction we should still see them, because they are committed
        self.assertEquals([1, 2], list(SQL.select_column("select bar from foo")))

    def test_basic_rollback(self):
        # basic
        with self.assertRaises(TestException):
            with SQL.transaction():
                SQL.insert("foo", {"bar": 1})
                SQL.insert("foo", {"bar": 2})

                # within the transaction we should see what we inserted
                self.assertEquals(
                    [1, 2], list(SQL.select_column("select bar from foo"))
                )
                raise TestException("a mystery")

        # after/outside the transaction we should not because it gets r ollwed back
        self.assertEquals([], list(SQL.select_column("select bar from foo")))

    def test_nested_rollback(self):
        with self.assertRaises(TestException):
            with SQL.transaction():
                SQL.insert("foo", {"bar": 1})
                SQL.insert("foo", {"bar": 2})

                # within the transaction we should see what we inserted
                self.assertEquals(
                    [1, 2], list(SQL.select_column("select bar from foo"))
                )

                with SQL.transaction():
                    SQL.insert("foo", {"bar": 3})
                    self.assertEquals(
                        [1, 2, 3], list(SQL.select_column("select bar from foo"))
                    )

                raise TestException("a mystery")

        # after/outside the transaction we should not because it gets r ollwed back
        self.assertEquals([], list(SQL.select_column("select bar from foo")))

    def test_nested_rollback2(self):
        with self.assertRaises(TestException):
            with SQL.transaction():
                SQL.insert("foo", {"bar": 1})
                SQL.insert("foo", {"bar": 2})

                # within the transaction we should see what we inserted
                self.assertEquals(
                    [1, 2], list(SQL.select_column("select bar from foo"))
                )

                with SQL.transaction():
                    SQL.insert("foo", {"bar": 3})
                    self.assertEquals(
                        [1, 2, 3], list(SQL.select_column("select bar from foo"))
                    )
                    raise TestException("a mystery")

        # after/outside the transaction we should not because it gets r ollwed back
        self.assertEquals([], list(SQL.select_column("select bar from foo")))

    def test_nested_commit(self):
        with SQL.transaction():
            SQL.insert("foo", {"bar": 1})
            SQL.insert("foo", {"bar": 2})

            # within the transaction we should see what we inserted
            self.assertEquals([1, 2], list(SQL.select_column("select bar from foo")))

            with SQL.transaction():
                SQL.insert("foo", {"bar": 3})
                self.assertEquals(
                    [1, 2, 3], list(SQL.select_column("select bar from foo"))
                )

        # after/outside the transaction we should not because it gets r ollwed back
        self.assertEquals([1, 2, 3], list(SQL.select_column("select bar from foo")))

    def test_bulk_insert(self):
        num = 10
        data = [{"bar": c} for c in range(num)]
        SQL.insert("foo", data)
        vals = list(SQL.select_column("select bar from foo order by bar"))
        self.assertEqual(vals, list(range(num)))

    def test_execute_fail(self):
        with self.assertRaises(Exception):
            SQL.execute("gibberish", message="more gibberish")

class TestMigration(unittest.TestCase):
    def setUp(self):
        SQL.execute("drop table if exists migrations")
        SQL.execute("drop table if exists test1")
        SQL.execute("drop table if exists test2")

    def test_migrate(self):
        logging.basicConfig(level=logging.ERROR)

        initial = Migration(1, "initial version")
        initial.add_statement("drop table if exists test1")
        initial.add_statement("drop table if exists test2")
        initial.add_statement("create table test1(bar integer, baz varchar(20))")

        new = Migration(2, "next version")
        new.add_statement("drop table if exists test2")
        new.add_statement("create table test2(bar integer, baz varchar(20))")

        # ok these are really just smoke tests
        # we could look at the logs and comapre to expected values if needed...
        # logs = Migration.migrate(SQL, dry_run=True)
        # logs = Migration.migrate(SQL, dry_run=True, initial=False)
        # logs = Migration.migrate(SQL, dry_run=True, initial=True)
        # logs = Migration.migrate(SQL, dry_run=True, initial=False, apply_versions=[2])

        # let's try a full migration
        print("exists", SQL.table_exists("test1"))
        self.assertFalse(SQL.table_exists("test1"))
        self.assertFalse(SQL.table_exists("test2"))
        logs = Migration.migrate(SQL, dry_run=False)
        self.assertTrue(SQL.table_exists("test1"))
        self.assertTrue(SQL.table_exists("test2"))

        # let's run an initial=True which should start from scratch again
        self.assertTrue(SQL.table_exists("test1"))
        self.assertTrue(SQL.table_exists("test2"))
        logs = Migration.migrate(SQL, dry_run=False, initial=True)
        self.assertTrue(SQL.table_exists("test1"))
        self.assertTrue(SQL.table_exists("test2"))

        # let's delete one table and run just a specific version
        SQL.execute("drop table test2")
        self.assertTrue(SQL.table_exists("test1"))
        self.assertFalse(SQL.table_exists("test2"))
        logs = Migration.migrate(SQL, dry_run=False, initial=False, apply_versions=[2])
        self.assertTrue(SQL.table_exists("test1"))
        self.assertTrue(SQL.table_exists("test2"))
 */
