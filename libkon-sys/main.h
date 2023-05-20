#ifdef __cplusplus
#include <duckdb.hpp>
#else
#include "arrow.h"
#include <duckdb.h>
#include <stdbool.h>
#endif

#ifdef __cplusplus
extern "C"
{
#endif

    typedef void (*drop_arrow_stream_factory)(void *);
    typedef void (*create_arrow_stream)(void *, struct ArrowArrayStream *);

    void new_in_memory(duckdb_database *db_out, duckdb_connection *conn_out);
    void destroy(duckdb_database *db, duckdb_connection *conn);
    void query(duckdb_connection connection, const char *sql);

    void register_arrow_stream(duckdb_connection, const char *, create_arrow_stream, drop_arrow_stream_factory, void *);

#ifdef __cplusplus
}
#endif