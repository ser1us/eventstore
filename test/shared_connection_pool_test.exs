defmodule EventStore.SharedConnectionPoolTest do
  use EventStore.StorageCase

  alias EventStore.EventFactory
  alias EventStore.MonitoredServer.State, as: MonitoredServerState

  describe "connection pool sharing" do
    setup do
      start_supervised!(
        {TestEventStore, name: :eventstore1, shared_connection_pool: :shared_pool}
      )

      start_supervised!(
        {TestEventStore, name: :eventstore2, shared_connection_pool: :shared_pool}
      )

      :ok
    end

    test "should only start one Postgrex connection pool" do
      pid1 = Process.whereis(Module.concat([:eventstore1, Postgrex]))

      assert is_pid(pid1)
      assert %MonitoredServerState{pid: conn} = :sys.get_state(pid1)

      pid2 = Process.whereis(Module.concat([:eventstore2, Postgrex]))

      assert is_pid(pid2)
      assert %MonitoredServerState{pid: ^conn} = :sys.get_state(pid2)

      # Event stores sharing a connection pool should use the same `Postgrex` conn
      assert conn == Process.whereis(:shared_pool)

      # An event store started without specifying a connection pool should start its own pool
      pid = Process.whereis(Module.concat([TestEventStore, Postgrex]))
      assert is_pid(pid)

      # Rudimentary check that this is a `Postgrex` process based on its state
      assert {:ready, _ref, _state} = :sys.get_state(pid)
    end

    test "should append and read events" do
      stream_uuid = UUID.uuid4()

      {:ok, events} = append_events_to_stream(:eventstore1, stream_uuid, 3)

      assert_recorded_events(:eventstore1, stream_uuid, events)
      assert_recorded_events(:eventstore2, stream_uuid, events)
    end
  end

  defp append_events_to_stream(event_store_name, stream_uuid, count, expected_version \\ 0) do
    events = EventFactory.create_events(count, expected_version + 1)

    :ok =
      TestEventStore.append_to_stream(stream_uuid, expected_version, events,
        name: event_store_name
      )

    {:ok, events}
  end

  defp assert_recorded_events(event_store_name, stream_uuid, expected_events) do
    actual_events =
      TestEventStore.stream_forward(stream_uuid, 0, name: event_store_name) |> Enum.to_list()

    assert_events(expected_events, actual_events)
  end

  defp assert_events(expected_events, actual_events) do
    assert length(expected_events) == length(actual_events)

    for {expected, actual} <- Enum.zip(expected_events, actual_events) do
      assert_event(expected, actual)
    end
  end

  defp assert_event(expected_event, actual_event) do
    assert expected_event.correlation_id == actual_event.correlation_id
    assert expected_event.causation_id == actual_event.causation_id
    assert expected_event.event_type == actual_event.event_type
    assert expected_event.data == actual_event.data
    assert expected_event.metadata == actual_event.metadata
  end
end
