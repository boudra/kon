#include "main.h"
#include <vector>

struct ArrowProjectedColumns
{
  std::unordered_map<idx_t, std::string> projection_map;
  std::vector<std::string> columns;
};

struct ArrowStreamParameters
{
  ArrowProjectedColumns projected_columns;
  duckdb::TableFilterSet *filters;
};

class ArrowStreamFactory : public duckdb::ExternalDependency
{
  create_arrow_stream create;
  drop_arrow_stream_factory drop;
  void *private_data;

public:
  ArrowStreamFactory(create_arrow_stream create, drop_arrow_stream_factory drop, void *private_data) : duckdb::ExternalDependency(duckdb::PYTHON_DEPENDENCY), create(create), drop(drop), private_data(private_data)
  {
  }
  ~ArrowStreamFactory()
  {
    this->drop(this->private_data);
  }

  std::unique_ptr<duckdb::ArrowArrayStreamWrapper> GetStream()
  {
    //! Export C arrow stream stream
    auto stream_wrapper =
        duckdb::make_unique<duckdb::ArrowArrayStreamWrapper>();

    this->create(this->private_data, &stream_wrapper->arrow_array_stream);

    return stream_wrapper;
  }

  static std::unique_ptr<duckdb::ArrowArrayStreamWrapper>
  CreateStream(uintptr_t this_ptr, ArrowStreamParameters &parameters)
  {
    //! Create a new batch reader
    auto &factory = *reinterpret_cast<ArrowStreamFactory *>(this_ptr); //! NOLINT

    return factory.GetStream();
  }

  static void GetSchema(uintptr_t factory_ptr,
                        duckdb::ArrowSchemaWrapper &schema)
  {
    auto &factory = *reinterpret_cast<ArrowStreamFactory *>(factory_ptr); //! NOLINT

    auto stream_wrapper = factory.GetStream();

    //! Pass ownership to caller
    stream_wrapper->arrow_array_stream.get_schema(
        &stream_wrapper->arrow_array_stream, &schema.arrow_schema);
  }
};

extern "C"
{
  void destroy(duckdb_database *db, duckdb_connection *conn)
  {
    duckdb_disconnect(conn);
    duckdb_close(db);
  }

  void new_in_memory(duckdb_database *db_out, duckdb_connection *conn_out)
  {
    duckdb_open(nullptr, db_out);
    duckdb_connect(*db_out, conn_out);
  }

  void query(duckdb_connection connection, const char *sql)
  {
    duckdb::Connection *conn = (duckdb::Connection *)(connection);
    conn->Query(sql)->Print();
  }

  void register_arrow_stream(duckdb_connection connection, const char *name, create_arrow_stream create, drop_arrow_stream_factory drop, void *private_data)
  {
    auto factory = std::make_shared<ArrowStreamFactory>(create, drop, private_data);

    duckdb::Connection *conn = (duckdb::Connection *)(connection);

    duckdb::vector<duckdb::Value> params;
    params.push_back(duckdb::Value::POINTER((uintptr_t)factory.get()));
    params.push_back(
        duckdb::Value::POINTER((uintptr_t)&ArrowStreamFactory::CreateStream));
    params.push_back(
        duckdb::Value::POINTER((uintptr_t)&ArrowStreamFactory::GetSchema));

    auto table = conn->TableFunction("arrow_scan", params);
    auto relation = table->CreateView(name, true, true);

    conn->context->external_dependencies["konbert"].push_back(std::dynamic_pointer_cast<duckdb::ExternalDependency>(factory));
  }
}