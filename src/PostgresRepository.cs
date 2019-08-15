﻿using System;
using System.Data;
using System.Text;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using System.Collections.Generic;

using Dapper;
using Npgsql;

namespace GlitchedPolygons.RepositoryPattern.Postgres
{
    /// <summary>
    /// Repository base class for PostgreSQL databases.
    /// Implements the <see cref="IRepository{T1, T2}" /> interface.
    /// </summary>
    /// <typeparam name="T1">The type of entity stored in the repository. The name of this type should also exactly match the name of the db table!</typeparam>
    /// <typeparam name="T2">The type of unique id with which the entities will be identified inside the repo (typically this will be either a <c>string</c> containing a GUID or an auto-incremented <c>int</c>).</typeparam>
    /// <seealso cref="IRepository{T1, T2}" />
    public abstract class PostgresRepository<T1, T2> : IRepository<T1, T2> where T1 : IEntity<T2>
    {
    	/// <summary>
        /// The name of the underlying PostgreSQL schema (default is "public").
        /// </summary>
    	public string SchemaName { get; }

        /// <summary>
        /// The name of the underlying PostgreSQL database table where the repository entities are stored.
        /// </summary>
        public string TableName { get; }

        private readonly string connectionString;
        private readonly string getByIdSql, getAllSql, deleteByIdSql, deleteAllSql;
        
        /// <summary>
        /// Creates a new PostgreSQL repository using a specific connection string.
        /// </summary>
        /// <param name="connectionString">The postgres db connection string. Ensure that this is valid!</param>
        /// <param name="schemaName">Optional PostgreSQL schema. If left out, the default schema ("public") is used.</param>
        /// <param name="tableName">Optional custom name for the underlying PostgreSQL database table. If left out, the entity's type name is used.</param>
        protected PostgresRepository(string connectionString, string schemaName = null, string tableName = null)
        {
        	this.SchemaName = string.IsNullOrEmpty(schemaName) ? "public" : schemaName;
            this.TableName = string.IsNullOrEmpty(tableName) ? typeof(T1).Name : tableName;
            this.connectionString = connectionString;

            getAllSql = $"SELECT * FROM {SchemaName}.\"{TableName}\"";
            getByIdSql = $"SELECT * FROM {SchemaName}.\"{TableName}\" WHERE \"Id\" = @Id";
            deleteAllSql = $"DELETE FROM {SchemaName}.\"{TableName}\"";
            deleteByIdSql = $"DELETE FROM {SchemaName}.\"{TableName}\" WHERE \"Id\" = @Id";
        }

        /// <summary>
        /// Opens a <see cref="IDbConnection"/> to the PostgreSQL database. <para> </para>
        /// Does not dispose automatically: make sure to wrap your usage into a <c>using</c> block.
        /// </summary>
        /// <returns>The opened <see cref="IDbConnection"/> (remember to dispose of it asap after you're done!).</returns>
        protected IDbConnection OpenConnection()
        {
            var npgsqlConnection = new NpgsqlConnection(connectionString);
            npgsqlConnection.Open();
            return npgsqlConnection;
        }

        /// <summary>
        /// Gets an entity by its unique identifier.
        /// </summary>
        /// <param name="id">The entity's unique identifier.</param>
        /// <returns>The first found <see cref="T:GlitchedPolygons.RepositoryPattern.IEntity`1" />; <c>null</c> if nothing was found.</returns>
        public async Task<T1> Get(T2 id)
        {
            using (var sqlc = OpenConnection())
            {
                return await sqlc.QueryFirstOrDefaultAsync<T1>(getByIdSql, new { Id = id });
            }
        }

        /// <summary>
        /// Gets an entity by its unique identifier.
        /// </summary>
        /// <param name="id">The entity's unique identifier.</param>
        /// <returns>The first found <see cref="T:GlitchedPolygons.RepositoryPattern.IEntity`1" />; <c>null</c> if nothing was found.</returns>
        public T1 this[T2 id]
        {
            get
            {
                using (var sqlc = OpenConnection())
                {
                    return sqlc.QueryFirstOrDefault<T1>(getByIdSql, new { Id = id });
                }
            }
        }

        /// <summary>
        /// Gets all entities from the repository.
        /// </summary>
        /// <returns>All entities inside the repo.</returns>
        public async Task<IEnumerable<T1>> GetAll()
        {
            using (var sqlc = OpenConnection())
            {
                return await sqlc.QueryAsync<T1>(getAllSql);
            }
        }

        /// <summary>
        /// Gets a single entity from the repo according to the specified predicate condition.<para> </para>
        /// If 0 or &gt;1 entities are found, <c>null</c> is returned.<para> </para>
        /// WARNING: Can be very slow! For specialized queries, just create a new repository! Derive from this class and create custom SQL queries that return the mapped types that you need.
        /// </summary>
        /// <param name="predicate">The search predicate.</param>
        /// <returns>Single found entity; <c>default(<typeparamref name="T1"/>)</c> (usually this is <c>null</c>) if 0 or &gt;1 entities were found.</returns>
        public async Task<T1> SingleOrDefault(Expression<Func<T1, bool>> predicate)
        {
            try
            {
                T1 result = (await GetAll()).SingleOrDefault(predicate.Compile());
                return result;
            }
            catch (Exception)
            {
                return default;
            }
        }

        /// <summary>
        /// Finds all entities according to the specified predicate <see cref="T:System.Linq.Expressions.Expression" />.<para> </para>
        /// WARNING: Can be very slow! For specialized queries, just create a new repository! Derive from this class and create custom SQL queries that return the mapped types that you need.
        /// </summary>
        /// <param name="predicate">The search predicate (all entities that match the provided conditions will be added to the query's result).</param>
        /// <returns>The found entities (<see cref="T:System.Collections.Generic.IEnumerable`1" />).</returns>
        public async Task<IEnumerable<T1>> Find(Expression<Func<T1, bool>> predicate)
        {
            try
            {
                IEnumerable<T1> result = (await GetAll()).Where(predicate.Compile());
                return result;
            }
            catch (Exception)
            {
                return Array.Empty<T1>();
            }
        }

        /// <summary>
        /// Adds the specified entity to the data repository. <para> </para>
        /// You need to ENSURE the uniqueness of the addendum <see cref="IEntity{T}.Id"/>!
        /// </summary>
        /// <param name="entity">The entity to add.</param>
        /// <returns>Whether the entity could be added successfully or not.</returns>
        public abstract Task<bool> Add(T1 entity);

        /// <summary>
        /// Adds multiple entities at once.<para> </para>
        /// You need to ENSURE the uniqueness of each added <see cref="IEntity{T}.Id"/>!
        /// </summary>
        /// <param name="entities">The entities to add.</param>
        /// <returns>Whether the entities were added successfully or not.</returns>
        public abstract Task<bool> AddRange(IEnumerable<T1> entities);

        /// <summary>
        /// Updates the specified entity.
        /// </summary>
        /// <param name="entity">The entity to update.</param>
        /// <returns>Whether the entity could be updated successfully or not.</returns>
        public abstract Task<bool> Update(T1 entity);

        /// <summary>
        /// Removes the specified entity.
        /// </summary>
        /// <param name="entity">The entity to remove.</param>
        /// <returns>Whether the entity could be removed successfully or not.</returns>
        public async Task<bool> Remove(T1 entity)
        {
            return await Remove(entity.Id);
        }

        /// <summary>
        /// Removes the specified entity.
        /// </summary>
        /// <param name="id">The unique id of the entity to remove.</param>
        /// <returns>Whether the entity could be removed successfully or not.</returns>
        public async Task<bool> Remove(T2 id)
        {
            bool result = false;
            IDbConnection sqlc = null;

            try
            {
                sqlc = OpenConnection();
                result = await sqlc.ExecuteAsync(deleteByIdSql, new { Id = id }) > 0;
            }
            catch (Exception)
            {
                result = false;
            }
            finally
            {
                sqlc?.Dispose();
            }

            return result;
        }

        /// <summary>
        /// Removes all entities at once from the repository.
        /// </summary>
        /// <returns>Whether the entities were removed successfully or not. If the repository was already empty, <c>false</c> is returned (because nothing was actually &lt;&lt;removed&gt;&gt; ).</returns>
        public async Task<bool> RemoveAll()
        {
            bool result = false;
            IDbConnection sqlc = null;

            try
            {
                sqlc = OpenConnection();
                result = await sqlc.ExecuteAsync(deleteAllSql) > 0;
            }
            catch (Exception)
            {
                result = false;
            }
            finally
            {
                sqlc?.Dispose();
            }

            return result;
        }

        /// <summary>
        /// Removes all entities that match the specified conditions (via the predicate <see cref="T:System.Linq.Expressions.Expression" /> parameter).<para> </para>
        /// WARNING: Can be slow. If you know the entities' id, please use the other RemoveRange overloads!
        /// </summary>
        /// <param name="predicate">The predicate <see cref="T:System.Linq.Expressions.Expression" /> that defines which entities should be removed.</param>
        /// <returns>Whether the entities were removed successfully or not.</returns>
        public async Task<bool> RemoveRange(Expression<Func<T1, bool>> predicate)
        {
            return await RemoveRange(await Find(predicate));
        }

        /// <summary>
        /// Removes the range of entities from the repository.
        /// </summary>
        /// <param name="entities">The entities to remove.</param>
        /// <returns>Whether all entities were removed successfully or not.</returns>
        public async Task<bool> RemoveRange(IEnumerable<T1> entities)
        {
            return await RemoveRange(entities.Select(e => e.Id));
        }

        /// <summary>
        /// Removes the range of entities from the repository.
        /// </summary>
        /// <param name="ids">The unique ids of the entities to remove.</param>
        /// <returns>Whether all entities were removed successfully or not.</returns>
        public async Task<bool> RemoveRange(IEnumerable<T2> ids)
        {
            bool result = false;
            IDbConnection sqlc = null;

            try
            {
                var sql = new StringBuilder(256)
                    .Append("DELETE FROM ").Append(SchemaName).Append('.')
                    .Append('\"')
                    .Append(TableName)
                    .Append('\"')
                    .Append(" WHERE \"Id\" IN (");

                foreach (T2 id in ids)
                {
                    sql.Append('\'').Append(id).Append('\'').Append(", ");
                }

                sqlc = OpenConnection();
                result = await sqlc.ExecuteAsync(sql.ToString().TrimEnd(',', ' ') + ");") > 0;
            }
            catch (Exception)
            {
                result = false;
            }
            finally
            {
                sqlc?.Dispose();
            }

            return result;
        }
    }
}
