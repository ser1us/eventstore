defmodule MigrationSourceTest do
  use ExUnit.Case

  alias EventStore.Config
  alias EventStore.Storage.Database

  setup_all do
    config = MigrationSourceEventStore.config()

    [config: config]
  end

  test "test migration source has the correct name and retuns a value", %{config: config} do
    # construct a query
    migration_source = Config.get_migration_source(config)
    schema = Keyword.get(config, :schema)

    table = table_name(migration_source, schema)

    script = "select major_version, minor_version, patch_version from #{table}"

    # get the result from the database
    {:ok, result} = Database.execute_query(config, script)
    [major, minor, patch] = List.first(result.rows)

    # this verifies that we actually have values for major, minor & patch
    assert major >= 0
    assert minor >= 0
    assert patch >= 0
  end

  defp table_name(migration_source, nil), do: migration_source
  defp table_name(migration_source, schema), do: "#{schema}.#{migration_source}"
end