defmodule JobsPoolTest do
  use ExUnit.Case, async: true

  test "simple sync job" do
    {:ok, jobs} = JobsPool.start_link(10)
    ret = JobsPool.run!(jobs, fn -> 1 end, 1000)
    assert ret == 1
  end

  test "parallel sync jobs" do
    {:ok, agent} = Agent.start_link(fn -> 0 end)
    {:ok, jobs} = JobsPool.start_link(10)
    increment = fn -> Agent.update(agent, &(&1 + 1)) end
    Enum.each 1..99, fn(_) ->
      spawn_link fn ->
        JobsPool.run!(jobs, increment)
      end
    end
    JobsPool.run!(jobs, increment)
    JobsPool.join(jobs)
    assert Agent.get(agent, fn value -> value end) == 100
  end

  test "parallel async jobs" do
    {:ok, agent} = Agent.start_link(fn -> 0 end)
    {:ok, jobs} = JobsPool.start_link(10)
    increment = fn -> Agent.update(agent, &(&1 + 1)) end
    Enum.each 1..100, fn(_) ->
      JobsPool.async(jobs, increment)
    end
    JobsPool.join(jobs)
    assert Agent.get(agent, fn value -> value end) == 100
  end

  test "tasks crashes bubble up to caller" do
    {:ok, jobs} = JobsPool.start_link(10)
    assert_raise RuntimeError, fn ->
      JobsPool.run!(jobs, fn -> raise "foo" end, 1000)
    end
  end

  test "tasks crashes don't break the pool" do
    {:ok, jobs} = JobsPool.start_link(10)
    assert_raise RuntimeError, fn ->
      JobsPool.run!(jobs, fn -> raise "foo" end, 1000)
    end
    ret = JobsPool.run!(jobs, fn -> 1 end, 1000)
    assert ret == 1
  end

  test "jobs with explicit keys don't mix up" do
    {:ok, jobs} = JobsPool.start_link(3)
    Enum.each 1..10, fn(_) ->
      spawn_link fn ->
        uid = UUID.uuid4()
        returned_uid = JobsPool.run!(jobs, fn -> uid end, uid)
        assert returned_uid == uid
      end
    end
    :timer.sleep(100)
    JobsPool.join(jobs)
  end

  test "tasks exits bubble up to caller" do
    {:ok, jobs} = JobsPool.start_link(10)
    assert catch_exit(JobsPool.run!(jobs, fn -> exit 1 end)) == 1
  end

  test "tasks exits don't break the pool" do
    {:ok, jobs} = JobsPool.start_link(10)
    assert catch_exit(JobsPool.run!(jobs, fn -> exit 1 end)) == 1
    assert JobsPool.run!(jobs, fn -> 1 end) == 1
  end

  test "tasks throws bubble up to caller" do
    {:ok, jobs} = JobsPool.start_link(10)
    assert catch_throw(JobsPool.run!(jobs, fn -> throw 1 end)) == 1
  end

  test "tasks throws don't break the pool" do
    {:ok, jobs} = JobsPool.start_link(10)
    assert catch_throw(JobsPool.run!(jobs, fn -> throw 1 end)) == 1
    assert JobsPool.run!(jobs, fn -> 1 end) == 1
  end

  test "exit stack is preserved" do
    {:ok, jobs} = JobsPool.start_link(10)
    try do
      JobsPool.run!(jobs, fn -> exit 1 end)
    catch
      :exit, 1 ->
        stack = System.stacktrace()
        frame = Enum.at(stack, 0)
        assert {JobsPoolTest, _, 0, [file: 'test/jobspool_test.exs', line: _]} = frame
        frame = Enum.at(stack, 1)
        assert {JobsPool, _, 4, [file: 'lib/jobspool.ex', line: _]} = frame
      class, term ->
        raise "expected {:exit, 1}, got {#{inspect class}, #{inspect term}}"
    end
  end

  test "throw stack is preserved" do
    {:ok, jobs} = JobsPool.start_link(10)
    try do
      JobsPool.run!(jobs, fn -> throw 1 end)
    catch
      :throw, 1 ->
        stack = System.stacktrace()
        frame = Enum.at(stack, 0)
        assert {JobsPoolTest, _, 0, [file: 'test/jobspool_test.exs', line: _]} = frame
        frame = Enum.at(stack, 1)
        assert {JobsPool, _, 4, [file: 'lib/jobspool.ex', line: _]} = frame
      class, term ->
        raise "expected {:throw, 1}, got {#{inspect class}, #{inspect term}}"
    end
  end

  test "mfa run! form" do
    {:ok, jobs} = JobsPool.start_link(10)
    assert JobsPool.run!(jobs, {JobsPoolTest, :identity, [1]}) == 1
  end

  test "mfa async form" do
    {:ok, agent} = Agent.start_link(fn -> 0 end)
    {:ok, jobs} = JobsPool.start_link(10)
    JobsPool.async(jobs, {JobsPoolTest, :increment, [agent]})
    JobsPool.join(jobs)
    assert Agent.get(agent, fn value -> value end) == 1
  end

  def identity(arg), do: arg
  def increment(agent), do: Agent.update(agent, &(&1 + 1))
end
