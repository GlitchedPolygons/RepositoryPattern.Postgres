using System;
using System.Data;
using Dapper;
using GlitchedPolygons.RepositoryPattern;
using Npgsql;
using Xunit;

namespace Tests
{
    public class PostgresRepositoryTests
    {
        #region

        private const string DROP_TEST_TABLE_SQL = "        DROP TABLE IF EXISTS public.test_table;";

        private const string CREATE_TEST_TABLE_SQL = @"
                                                                    CREATE TABLE IF NOT EXISTS public.test_table
                                                                    (
                                                                        ""Id"" bigserial NOT NULL,
                                                                        test_int bigint,
                                                                        test_boolean boolean,
                                                                        test_double double precision,
                                                                        PRIMARY KEY (""Id"")
                                                                    )
                                                                    WITH (
                                                                        OIDS = FALSE
                                                                    );
        
                                                                    ALTER TABLE public.test_table OWNER to postgres;";

        private class TestClass : IEntity<long>
        {
            public long Id { get; set; }
            public long test_int { get; set; }
            public bool test_boolean { get; set; }
            public double test_double { get; set; }
        }

        #endregion

        private readonly IDbConnection dbConnection;
        private readonly IRepository<TestClass, long> repository;

        public PostgresRepositoryTests()
        {
            dbConnection = OpenConnection($"Server=127.0.0.1;Port=5432;User Id=postgres_repository_tests;Password=postgres_repository_tests;Database=postgres_repository_test_db");
            dbConnection.Execute(DROP_TEST_TABLE_SQL);
            dbConnection.Execute(CREATE_TEST_TABLE_SQL);
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
        public void Test1()
        {
            int c = dbConnection.QuerySingleOrDefault<int>("SELECT * FROM test_table COUNT");
            Assert.Equal(0, c);
        }
    }
}