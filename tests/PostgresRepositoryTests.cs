using System;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

using Xunit;
using Dapper;
using Npgsql;
using GlitchedPolygons.RepositoryPattern;

namespace Tests
{
    public class PostgresRepositoryTests : IDisposable
    {
        #region

        private const string DROP_TEST_TABLE_SQL = "DROP TABLE IF EXISTS public.\"TestClass\";";

        private const string CREATE_TEST_TABLE_SQL = @"

CREATE TABLE IF NOT EXISTS public.""TestClass""
(
    ""Id"" bigserial NOT NULL,
    ""TestLong"" bigint,
    ""TestBool"" boolean,
    ""TestDouble"" double precision,
    ""TestString"" text,
    PRIMARY KEY (""Id"")
);
";

        #endregion

        private readonly IDbConnection dbConnection;
        private readonly IRepository<TestClass, long> repository;

        public PostgresRepositoryTests()
        {
            var connectionString = $"Server=127.0.0.1;Port=5432;User Id=postgres_repository_tests;Password=postgres_repository_tests;Database=postgres_repository_test_db";
            dbConnection = OpenConnection(connectionString);
            dbConnection.Execute(DROP_TEST_TABLE_SQL);
            dbConnection.Execute(CREATE_TEST_TABLE_SQL);
            repository = new TestClassRepo(connectionString);
        }

        ~PostgresRepositoryTests()
        {
            Dispose();
        }

        public void Dispose()
        {
            dbConnection.Execute(DROP_TEST_TABLE_SQL);
            dbConnection.Close();
        }

        private IDbConnection OpenConnection(string connectionString)
        {
            var npgsqlConnection = new NpgsqlConnection(connectionString);
            npgsqlConnection.Open();
            return npgsqlConnection;
        }

        private static bool AlmostEqual(double n, double m)
        {
            return Math.Abs(n - m) < 0.001D;
        }

        [Fact]
        public async Task AddRow_InsertedDataShouldBeCorrect()
        {
            var add = new TestClass
            {
                // Id is auto-incremented
                TestBool = true,
                TestLong = 1337,
                TestDouble = 420.69D,
                TestString = "Sauce???"
            };

            Assert.True(await repository.Add(add));
            Assert.Equal(1, dbConnection.QuerySingleOrDefault<int>($"SELECT COUNT(*) FROM \"{nameof(TestClass)}\""));

            TestClass get = dbConnection.QueryFirstOrDefault<TestClass>("SELECT * FROM public.\"TestClass\"");

            Assert.NotNull(get);
            Assert.True(get.TestBool);
            Assert.Equal(1337, get.TestLong);
            Assert.True(Math.Abs(get.TestDouble - 420.69D) < 0.01D);
            Assert.Equal("Sauce???", get.TestString);
        }

        [Fact]
        public async Task GetRow_ShouldRetrieveCorrectData()
        {
            await repository.Add(new TestClass
            {
                TestBool = true,
                TestLong = -133742069,
                TestDouble = -666.666D,
                TestString = "SeeS!!!"
            });

            TestClass test = await repository.Get(1);

            Assert.NotNull(test);
            Assert.True(test.TestBool);
            Assert.Equal(-133742069, test.TestLong);
            Assert.True(AlmostEqual(test.TestDouble, -666.666D));
            Assert.Equal("SeeS!!!", test.TestString);
        }

        [Fact]
        public void GetRowSync_ShouldRetrieveCorrectData()
        {
            repository.Add(new TestClass
            {
                TestBool = true,
                TestLong = -133742069,
                TestDouble = -666.666D,
                TestString = "SeeS!!!"
            }).GetAwaiter().GetResult();

            TestClass test = repository[1];

            Assert.NotNull(test);
            Assert.True(test.TestBool);
            Assert.Equal(-133742069, test.TestLong);
            Assert.True(AlmostEqual(test.TestDouble, -666.666D));
            Assert.Equal("SeeS!!!", test.TestString);
        }

        [Fact]
        public async Task GetAllRows_ShouldRetrieveCorrectData()
        {
            await repository.Add(new TestClass
            {
                TestBool = false,
                TestLong = -1,
                TestDouble = -1.0D,
                TestString = "-1"
            });

            await repository.Add(new TestClass
            {
                TestBool = true,
                TestLong = -2,
                TestDouble = -2.0D,
                TestString = "-2"
            });

            await repository.Add(new TestClass
            {
                TestBool = false,
                TestLong = -3,
                TestDouble = -3.0D,
                TestString = "-3"
            });

            TestClass[] test = (await repository.GetAll()).ToArray();

            Assert.NotNull(test);
            Assert.NotEmpty(test);
            Assert.Equal(3, test.Length);

            for (int i = 0; i < test.Length; i++)
            {
                var t = test[i];
                Assert.True(t.TestBool == (t.Id % 2 == 0));
                Assert.Equal(t.TestLong, -(i + 1));
                Assert.Equal(t.TestString, (-(i + 1)).ToString());
                Assert.True(AlmostEqual(t.TestDouble, -(i + 1)));
            }
        }

        [Fact]
        public async Task RemoveRowById_RowShouldNotExistAnymore_NextRowHasCorrectlyAutoIncrementedId()
        {
            var t1 = new TestClass
            {
                TestBool = false,
                TestLong = 1,
                TestDouble = 1.0D,
                TestString = "1"
            };

            var t2 = new TestClass
            {
                TestBool = true,
                TestLong = 2,
                TestDouble = 2.0D,
                TestString = "2"
            };

            await repository.Add(t1);
            await repository.Add(t2);

            Assert.True(await repository.Remove(2));
            Assert.Null(await repository.Get(2));

            await repository.Add(t2);

            Assert.Null(await repository.Get(2));
            Assert.NotNull(await repository.Get(3));

            var _t2 = await repository.Get(3);
            Assert.True(_t2.TestBool);
            Assert.Equal("2", _t2.TestString);
            Assert.Equal(2, _t2.TestLong);
            Assert.True(AlmostEqual(_t2.TestDouble, 2.0D));
        }

        [Fact]
        public async Task RemoveAllRows_RowsShouldNotExistAnymore_ButAutoIncrementedIdResumesFromLastTailId()
        {
            var t1 = new TestClass
            {
                TestBool = false,
                TestLong = 1,
                TestDouble = 1.0D,
                TestString = "1"
            };

            var t2 = new TestClass
            {
                TestBool = true,
                TestLong = 2,
                TestDouble = 2.0D,
                TestString = "2"
            };

            await repository.Add(t1);
            await repository.Add(t2);

            Assert.True(await repository.RemoveAll());
            Assert.Null(await repository.Get(1));
            Assert.Null(await repository.Get(2));
            Assert.Equal(0, dbConnection.QuerySingleOrDefault<int>($"SELECT COUNT(*) FROM \"{nameof(TestClass)}\""));

            await repository.Add(t2);

            Assert.Null(await repository.Get(1));
            Assert.Null(await repository.Get(2));
            Assert.NotNull(await repository.Get(3));

            var _t2 = await repository.Get(3);
            Assert.True(_t2.TestBool);
            Assert.Equal(2, _t2.TestLong);
            Assert.Equal("2", _t2.TestString);
            Assert.True(AlmostEqual(_t2.TestDouble, 2.0D));
        }

        [Fact]
        public async Task RemoveRangeOfRows_RowsShouldNotExistAnymore_ButAutoIncrementedIdResumesFromLastTailId()
        {
            var t1 = new TestClass
            {
                TestBool = false,
                TestLong = 1,
                TestDouble = 1.0D,
                TestString = "1"
            };

            var t2 = new TestClass
            {
                TestBool = true,
                TestLong = 2,
                TestDouble = 2.0D,
                TestString = "2"
            };

            await repository.AddRange(new[] { t1, t2 });

            Assert.True(await repository.RemoveRange(new long[] { 1, 2 }));
            Assert.Null(await repository.Get(1));
            Assert.Null(await repository.Get(2));
            Assert.Equal(0, dbConnection.QuerySingleOrDefault<int>($"SELECT COUNT(*) FROM \"{nameof(TestClass)}\""));

            await repository.Add(t2);

            Assert.Null(await repository.Get(1));
            Assert.Null(await repository.Get(2));
            Assert.NotNull(await repository.Get(3));

            var _t2 = await repository.Get(3);
            Assert.True(_t2.TestBool);
            Assert.Equal("2", _t2.TestString);
            Assert.Equal(2, _t2.TestLong);
            Assert.True(AlmostEqual(_t2.TestDouble, 2.0D));
        }
    }
}