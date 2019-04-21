using System;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Collections.Generic;
using System.Threading.Tasks;

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
        private readonly string typeName;
        private readonly string connectionString;

        /// <summary>
        /// Creates a new PostgreSQL repository using a specific connection string.
        /// </summary>
        /// <param name="connectionString">The postgres db connection string. Ensure that this is valid!</param>
        protected PostgresRepository(string connectionString)
        {
            this.typeName = typeof(T1).Name;
            this.connectionString = connectionString;
        }

        /// <summary>
        /// Opens a <see cref="IDbConnection"/> to the PostgreSQL database. <para> </para>
        /// Does not dispose automatically: make sure to wrap your usage into a <see langowrd="using"/> block.
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
                string sql = $"SELECT * FROM public.\"{typeName}\" WHERE \"Id\" = @Id";
                return await sqlc.QueryFirstOrDefaultAsync<T1>(sql, new { Id = id });
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
                    string sql = $"SELECT * FROM public.\"{typeName}\" WHERE \"Id\" = @Id";
                    return sqlc.QueryFirstOrDefault<T1>(sql, new { Id = id });
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
                string sql = $"SELECT * FROM public.\"{typeName}\"";
                return await sqlc.QueryAsync<T1>(sql);
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
            using (var sqlc = OpenConnection())
            {
                try
                {
                    int affectedRows = await sqlc.ExecuteAsync($"DELETE FROM public.\"{typeName}\" WHERE \"Id\" = @Id", new { Id = entity.Id });
                    return affectedRows > 0;
                }
                catch (Exception)
                {
                    return false;
                }
            }
        }

        /// <summary>
        /// Removes the specified entity.
        /// </summary>
        /// <param name="id">The unique id of the entity to remove.</param>
        /// <returns>Whether the entity could be removed successfully or not.</returns>
        public async Task<bool> Remove(T2 id)
        {
            using (var sqlc = OpenConnection())
            {
                try
                {
                    int affectedRows = await sqlc.ExecuteAsync($"DELETE FROM public.\"{typeName}\" WHERE \"Id\" = @Id", new { Id = id });
                    return affectedRows > 0;
                }
                catch (Exception)
                {
                    return false;
                }
            }
        }

        /// <summary>
        /// Removes all entities at once from the repository.
        /// </summary>
        /// <returns>Whether the entities were removed successfully or not. If the repository was already empty, <c>false</c> is returned (because nothing was actually &lt;&lt;removed&gt;&gt; ).</returns>
        public async Task<bool> RemoveAll()
        {
            using (var sqlc = OpenConnection())
            {
                try
                {
                    int affectedRows = await sqlc.ExecuteAsync($"DELETE FROM public.\"{typeName}\"");
                    return affectedRows > 0;
                }
                catch (Exception)
                {
                    return false;
                }
            }
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
            try
            {
                bool success = true;
                var tasks = new List<Task>(16);
                foreach (var e in entities)
                {
                    tasks.Add(Task.Run(() =>
                    {
                        using (var sqlc = OpenConnection())
                        {
                            if (sqlc.Execute($"DELETE FROM public.\"{typeName}\" WHERE \"Id\" = @Id", new { Id = e.Id }) <= 0)
                            {
                                success = false;
                            }
                        }
                    }));
                }
                await Task.WhenAll(tasks);
                return success;
            }
            catch (Exception)
            {
                return false;
            }
        }

        /// <summary>
        /// Removes the range of entities from the repository.
        /// </summary>
        /// <param name="ids">The unique ids of the entities to remove.</param>
        /// <returns>Whether all entities were removed successfully or not.</returns>
        public async Task<bool> RemoveRange(IEnumerable<T2> ids)
        {
            try
            {
                bool success = true;
                var tasks = new List<Task>(16);
                foreach (T2 id in ids)
                {
                    tasks.Add(Task.Run(() =>
                    {
                        using (var sqlc = OpenConnection())
                        {
                            if (sqlc.Execute($"DELETE FROM public.\"{typeName}\" WHERE \"Id\" = @Id", new { Id = id }) <= 0)
                            {
                                success = false;
                            }
                        }
                    }));
                }
                await Task.WhenAll(tasks);
                return success;
            }
            catch (Exception)
            {
                return false;
            }
        }
    }
}
