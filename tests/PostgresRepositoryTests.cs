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

        [Fact]
        public async Task AddRow_InsertedDataShouldBeCorrect()
        {
            var test = new TestClass
            {
                // Id is auto-incremented
                TestBool = true,
                TestLong = 1337,
                TestDouble = 420.69D,
                TestString = "Sauce???"
            };
            Assert.True(await repository.Add(test));
            int c = dbConnection.QuerySingleOrDefault<int>($"SELECT COUNT(*) FROM \"{nameof(TestClass)}\"");
            Assert.Equal(1, c);
        }
    }
}