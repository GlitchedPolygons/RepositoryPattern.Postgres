using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using Dapper;
using GlitchedPolygons.RepositoryPattern;
using GlitchedPolygons.RepositoryPattern.Postgres;
using Npgsql;
using Xunit;

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
    }
}