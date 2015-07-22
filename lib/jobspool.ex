defmodule JobsPool do
  @moduledoc """
  A simple concurrent jobs pool.

  There are two use cases for this module:

  * Multiple processes spawning jobs in the same pool:

        {:ok, pool} = JobsPool.start_link(10)
        Enum.each 1..99, fn(_) ->
          spawn fn ->
            JobsPool.run!(pool, fn -> :timer.sleep(100) end)
          end
        end
        JobsPool.run!(pool, fn -> :timer.sleep(100) end)
        JobsPool.join(pool)     # Should take ~1s

    `JobsPool.run!/4` blocks until the job is done and returns the job's
    result, or reraise if it had an error.

  * One process spawning jobs asynchronously:

        {:ok, pool} = JobsPool.start_link(10)
        Enum.each 1..100, fn(_) ->
          JobsPool.async(pool, fn -> :timer.sleep(100) end)
        end
        JobsPool.join(pool)     # Should take ~1s

    There is currently no way to retrieve an async job's result.

  `JobsPool.start_link/2` second argument is an array of options passed to
  `GenServer.start_link/3`. For example to create a named pool:

      JobsPool.start_link(10, name: :jobs_pool)
      JobsPool.run!(:jobs_pool, fn -> :timer.sleep(100) end)

  """

  use GenServer

  defmodule State do
    defstruct [
      max_concurrent_jobs: 1,

      active_jobs: %{},
      queued_jobs: :queue.new(),
      queued_keys: HashSet.new(),
      waiters: %{},
      joiners: [],
    ]
  end

  # --------------------------------------------------------------------------
  # Public API

  @doc """

  """
  def start_link(max_concurrent_jobs, genserver_options \\ []) do
    state = %State{max_concurrent_jobs: max_concurrent_jobs}
    GenServer.start_link(__MODULE__, state, genserver_options)
  end

  @doc """
  Execute `fun` and block until it's complete, or `timeout` exceeded.

  Return `fun` return value, or raise in the current process if it encountered
  an error.
  """
  def run!(server, fun, key \\ nil, timeout \\ :infinity) do
    GenServer.call(server, {:run, fun, key}, timeout)
    |> maybe_reraise()
  end

  @doc """
  Execute `fun` asynchronously.

  Return the task key.
  """
  def async(server, fun, key \\ nil) do
    GenServer.call(server, {:async, fun, key})
  end

  @doc """
  Wait until all jobs are finished.
  """
  def join(server, timeout \\ :infinity) do
    GenServer.call(server, {:join}, timeout)
  end

  # --------------------------------------------------------------------------
  # GenServer implementation

  @doc false
  def handle_call({:run, fun, key}, from, state) do
    key = maybe_create_key(key)
    state = state
            |> run_job(fun, key)
            |> add_waiter(key, from)
    {:noreply, state}
  end

  @doc false
  def handle_call({:async, fun, key}, _from, state) do
    key = maybe_create_key(key)
    state = run_job(state, fun, key)
    {:reply, key, state}
  end

  @doc false
  def handle_call({:join}, from, state) do
    if Map.size(state.active_jobs) > 0 or :queue.len(state.queued_jobs) > 0 do
      state = add_joiner(state, from)
      {:noreply, state}
    else
      {:reply, :done, state}
    end
  end

  @doc false
  def handle_info(message, state) do
    active_tasks = Map.values(state.active_jobs)

    case Task.find(active_tasks, message) do
      {{key, result}, _task} ->
        state = state
                |> remove_active_job(key)
                |> notify_waiters(key, result)
                |> run_next_job()
      nil -> :ok
    end

    {:noreply, state}
  end

  # --------------------------------------------------------------------------
  # Helpers

  defp notify_waiters(state, key, result) do
    if Map.has_key?(state.waiters, key) do
      Enum.each state.waiters[key], fn(waiter) ->
        GenServer.reply(waiter, result)
      end
      update_in(state.waiters, &Map.delete(&1, key))
    else
      state
    end
  end

  defp add_waiter(state, key, waiter) do
    waiters = Map.get(state.waiters, key, HashSet.new())
              |> HashSet.put(waiter)
    update_in(state.waiters, &Map.put(&1, key, waiters))
  end

  defp remove_active_job(state, key) do
    update_in(state.active_jobs, &Map.delete(&1, key))
  end

  defp unqueue_job(state, new_queued_jobs, key) do
    state = put_in(state.queued_jobs, new_queued_jobs)
    update_in(state.queued_keys, &Set.delete(&1, key))
  end

  defp add_joiner(state, joiner) do
    update_in(state.joiners, &([joiner | &1]))
  end

  defp run_next_job(state) do
    case :queue.out(state.queued_jobs) do
      {{:value, {key, fun}}, new_queued_jobs} ->
        state
        |> unqueue_job(new_queued_jobs, key)
        |> run_job(fun, key)
      {:empty, _} ->
        if Map.size(state.active_jobs) == 0 do
          state
          |> notify_joiners()
          |> clear_joiners()
        else
          state
        end
    end
  end

  defp notify_joiners(state) do
    Enum.each state.joiners, fn(joiner) ->
      GenServer.reply(joiner, :done)
    end
    state
  end

  defp clear_joiners(state) do
    put_in(state.joiners, [])
  end

  defp maybe_create_key(key) do
    unless key do
      UUID.uuid4()
    end
  end

  defp run_job(state, fun, key) do
    # Check a job with `key` is not already active or queued
    if not Map.has_key?(state.active_jobs, key) and not Set.member?(state.queued_keys, key) do
      if Map.size(state.active_jobs) < state.max_concurrent_jobs do
        # There are slots available, execute job now

        # Wrap fun to catch exceptions, since we want to reraise them in the client
        # processes, not in this GenServer
        wrapped_fun = fn ->
          try do
            {key, {:ok, fun.()}}
          rescue
            exception ->
              stacktrace = System.stacktrace()
              {key, {:exception, exception, stacktrace}}
          end
        end

        # Create Task and put in in active jobs
        task = Task.async(wrapped_fun)
        state = update_in(state.active_jobs, &Map.put(&1, key, task))
      else
        # No slots available, queue job
        state = update_in(state.queued_jobs, &:queue.in({key, fun}, &1))
        state = update_in(state.queued_keys, &Set.put(&1, key))
      end
    end
    state
  end

  defp maybe_reraise({:ok, result}), do: result
  defp maybe_reraise({:exception, exception, stacktrace}), do: reraise(exception, stacktrace)
end