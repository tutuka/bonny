defmodule Bonny.ControllerV2Test do
  use ExUnit.Case, async: false

  alias Bonny.ControllerV2

  @conn %K8s.Conn{}
  @controller {Bonny.ControllerV2Test.Controller, []}
  @agent_name Bonny.ControllerV2Test.CountingAgent

  defmodule Controller do
    use Bonny.ControllerV2
    step :handle_event
    def handle_event(axn, _), do: axn
  end

  defmodule CountingOperator do
    def call(%Bonny.Axn{action: action, resource: resource} = axn, []) do
      name = resource["metadata"]["name"]
      Agent.update(Bonny.ControllerV2Test.CountingAgent, &(&1 ++ [{action, name}]))

      if name == "fail", do: raise("event failed")

      axn
    end
  end

  setup do
    if pid = Process.whereis(@agent_name), do: Agent.stop(pid)
    {:ok, _} = Agent.start_link(fn -> [] end, name: @agent_name)
    :ok
  end

  defp resource(name) do
    %{
      "apiVersion" => "v1",
      "kind" => "ConfigMap",
      "metadata" => %{"name" => name, "namespace" => "default"}
    }
  end

  defp watch_stream(events) do
    Stream.map(events, &Bonny.Operator.run(&1, @controller, CountingOperator, @conn))
  end

  describe "build_reconciler_stream/5" do
    test "halt_on_error false continues the list pass after failure" do
      resources = [resource("fail"), resource("ok-1"), resource("ok-2")]

      stream =
        ControllerV2.build_reconciler_stream(
          resources,
          @controller,
          CountingOperator,
          @conn,
          halt_on_error: false
        )

      assert [{:ok, :error}, {:ok, _}, {:ok, _}] = Enum.to_list(stream)

      assert MapSet.new(Agent.get(@agent_name, & &1)) ==
               MapSet.new([{:reconcile, "fail"}, {:reconcile, "ok-1"}, {:reconcile, "ok-2"}])
    end

    test "halt_on_error true stops the list pass on first failure" do
      resources = [resource("fail"), resource("ok-1")]

      stream =
        ControllerV2.build_reconciler_stream(
          resources,
          @controller,
          CountingOperator,
          @conn,
          halt_on_error: true,
          max_concurrency: 1
        )

      {_, ref} = spawn_monitor(fn -> Enum.to_list(stream) end)

      assert_receive {:DOWN, ^ref, :process, _, reason}, 5_000
      assert reason != :normal
      assert Agent.get(@agent_name, & &1) == [{:reconcile, "fail"}]
    end
  end

  describe "watch stream" do
    test "processes add, modify, and delete events" do
      events = [
        {:add, resource("new")},
        {:modify, resource("updated")},
        {:delete, resource("gone")}
      ]

      Stream.run(watch_stream(events))

      assert Agent.get(@agent_name, & &1) == [
               {:add, "new"},
               {:modify, "updated"},
               {:delete, "gone"}
             ]
    end

    test "failure crashes the stream (halt_on_error not applicable)" do
      events = [{:add, resource("ok")}, {:modify, resource("fail")}, {:delete, resource("never")}]

      {_, ref} = spawn_monitor(fn -> Stream.run(watch_stream(events)) end)

      assert_receive {:DOWN, ^ref, :process, _, reason}, 5_000
      assert reason != :normal
      assert Agent.get(@agent_name, & &1) == [{:add, "ok"}, {:modify, "fail"}]
    end
  end
end
