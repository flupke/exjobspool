defmodule JobsPool do
  @moduledoc """
  A simple concurrent jobs pool.

  You can spawn jobs synchronously:

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

  You can also spawn jobs asynchronously:

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
    @moduledoc false

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
  Start a `JobsPool` server with `max_concurrent_jobs` execution slots.

  `genserver_options` is passed to `GenServer.start_link/3`.
  """
  def start_link(max_concurrent_jobs, genserver_options \\ []) do
    state = %State{max_concurrent_jobs: max_concurrent_jobs}
    GenServer.start_link(__MODULE__, state, genserver_options)
  end

  @doc """
  Execute `job` and block until it's complete, or `timeout` exceeded.

  `job` can be an anonymous function with an arity of 0, or a `{mod, fun,
  args}` tuple.

  `key` can be used to avoid running the same job multiple times, only one job
  with the same key can be executed or queued at any given time. If no key is
  given, a random one is generated.

  Return `job` return value. Throws, raises and exits are bubbled up to the
  caller.
  """
  def run!(server, job, key \\ nil, timeout \\ :infinity)
  def run!(server, {mod, fun, args}, key, timeout) do
    GenServer.call(server, {:run, {mod, fun, args}, key}, timeout)
    |> maybe_reraise()
  end
  def run!(server, job, key, timeout) do
    GenServer.call(server, {:run, job, key}, timeout)
    |> maybe_reraise()
  end

  @doc """
  Execute `job` asynchronously.

  `job` can be an anonymous function with an arity of 0, or a `{mod, fun,
  args}` tuple.

  `key` can be used to avoid running the same job multiple times, only one job
  with the same key can be executed or queued at any given time. If no key is
  given, a random one is generated.

  Return the task key.
  """
  def async(server, job, key \\ nil)
  def async(server, {mod, fun, args}, key) do
    GenServer.call(server, {:async, {mod, fun, args}, key})
  end
  def async(server, job, key) do
    GenServer.call(server, {:async, job, key})
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
  def init(state) do
    Process.flag(:trap_exit, true)
    {:ok, state}
  end

  @doc false
  def handle_call({:run, job, key}, from, state) do
    key = maybe_create_key(key)
    state = state
            |> run_job(job, key)
            |> add_waiter(key, from)
    {:noreply, state}
  end

  @doc false
  def handle_call({:async, job, key}, _from, state) do
    key = maybe_create_key(key)
    state = run_job(state, job, key)
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
    active_keys = Map.keys(state.active_jobs)

    case Task.find(active_tasks, message) do
      {result, task} ->
        index = Enum.find_index(active_tasks, &(&1 == task))
        key = Enum.at(active_keys, index)
        state = state
                |> remove_active_job(key)
                |> notify_waiters(:ok, key, result)
                |> run_next_job()
      {:EXIT, task, reason} ->
        IO.puts "FOOOOOOOOOOOOOOOOOOOOO"
        index = Enum.find_index(active_tasks, &(&1 == task))
        key = Enum.at(active_keys, index)
        state = state
                |> remove_active_job(key)
                |> notify_waiters(:error, key, {:error, {reason, {__MODULE__, :run!, []}}})
                |> run_next_job()
      nil -> :ok
    end

    {:noreply, state}
  end

  # --------------------------------------------------------------------------
  # Helpers

  defp notify_waiters(state, kind, key, result) do
    if Map.has_key?(state.waiters, key) do
      Enum.each state.waiters[key], fn(waiter) ->
        GenServer.reply(waiter, {kind, result})
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
      {{:value, {key, job}}, new_queued_jobs} ->
        state
        |> unqueue_job(new_queued_jobs, key)
        |> run_job(job, key)
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

  defp maybe_create_key(nil), do: UUID.uuid4()
  defp maybe_create_key(key), do: key

  defp run_job(state, job, key) do
    # Check a job with `key` is not already active or queued
    if not Map.has_key?(state.active_jobs, key) and not Set.member?(state.queued_keys, key) do
      if Map.size(state.active_jobs) < state.max_concurrent_jobs do
        # There are slots available, execute job now
        case job do
          {mod, fun, args} ->
            task = Task.async(mod, fun, args)
          fun ->
            task = Task.async(fun)
        end
        state = update_in(state.active_jobs, &Map.put(&1, key, task))
      else
        # No slots available, queue job
        state = update_in(state.queued_jobs, &:queue.in({key, job}, &1))
        state = update_in(state.queued_keys, &Set.put(&1, key))
      end
    end
    state
  end

  defp maybe_reraise({:ok, result}), do: result
  defp maybe_reraise({:error, term}), do: exit(term)
end
