defmodule EventStore.Supervisor do
  @moduledoc false

  use Supervisor

  alias EventStore.{
    AdvisoryLocks,
    Config,
    MonitoredServer,
    Notifications,
    PubSub,
    Subscriptions
  }

  @doc """
  Starts the event store supervisor.
  """
  def start_link(event_store, otp_app, serializer, name, opts) do
    Supervisor.start_link(
      __MODULE__,
      {event_store, otp_app, serializer, name, opts},
      name: name
    )
  end

  @doc """
  Retrieves the compile time configuration.
  """
  def compile_config(event_store, opts) do
    otp_app = Keyword.fetch!(opts, :otp_app)
    config = Config.get(event_store, otp_app)

    {otp_app, config}
  end

  @doc """
  Retrieves the runtime configuration.
  """
  def runtime_config(event_store, otp_app, opts) do
    config =
      Application.get_env(otp_app, event_store, [])
      |> Keyword.merge(opts)
      |> Keyword.put(:otp_app, otp_app)

    case event_store_init(event_store, config) do
      {:ok, config} ->
        config = Config.parse(config)

        {:ok, config}

      :ignore ->
        :ignore
    end
  end

  ## Supervisor callbacks

  @doc false
  def init({event_store, otp_app, serializer, name, opts}) do
    case runtime_config(event_store, otp_app, opts) do
      {:ok, config} ->
        advisory_locks_name = Module.concat([name, AdvisoryLocks])
        advisory_locks_postgrex_name = Module.concat([advisory_locks_name, Postgrex])
        subscriptions_name = Module.concat([name, Subscriptions.Supervisor])
        subscriptions_registry_name = Module.concat([name, Subscriptions.Registry])

        children =
          [
            postgrex_connection_pool_child_spec(name, config),
            Supervisor.child_spec(
              {MonitoredServer,
               mfa: {Postgrex, :start_link, [Config.sync_connect_postgrex_opts(config)]},
               name: advisory_locks_postgrex_name},
              id: Module.concat([advisory_locks_postgrex_name, MonitoredServer])
            ),
            {AdvisoryLocks, conn: advisory_locks_postgrex_name, name: advisory_locks_name},
            {Subscriptions.Supervisor, name: subscriptions_name},
            Supervisor.child_spec({Registry, keys: :unique, name: subscriptions_registry_name},
              id: subscriptions_registry_name
            ),
            {Notifications.Supervisor, {name, serializer, config}}
          ] ++ PubSub.child_spec(name)

        Supervisor.init(children, strategy: :one_for_all)

      :ignore ->
        :ignore
    end
  end

  ## Private helpers

  defp event_store_init(event_store, config) do
    if Code.ensure_loaded?(event_store) and function_exported?(event_store, :init, 1) do
      event_store.init(config)
    else
      {:ok, config}
    end
  end

  # Get the child spec for the main Postgres dataabse connection pool.
  #
  # By default an event store instance will start its own connection pool. The
  # `:shared_connection_pool` config option can be used to share the same
  # database connection pool between multiple event store instances when they
  # connect to the same physical database. This will reduce the total number of
  # connections.
  defp postgrex_connection_pool_child_spec(name, config) do
    postgrex_name = Module.concat([name, Postgrex])

    case Keyword.get(config, :shared_connection_pool) do
      nil ->
        # Use a separate connection pool for event store instance
        {Postgrex, Config.postgrex_opts(config, postgrex_name)}

      shared_connection_pool when is_atom(shared_connection_pool) ->
        # Named connection pool that can be shared between event store instances
        Supervisor.child_spec(
          {MonitoredServer,
           mfa: {Postgrex, :start_link, [Config.postgrex_opts(config, shared_connection_pool)]},
           name: postgrex_name},
          id: Module.concat([postgrex_name, MonitoredServer])
        )

      invalid ->
        raise ArgumentError,
              "Invalid `:shared_connection_pool` specified, expected an atom but got: " <>
                inspect(invalid)
    end
  end
end
