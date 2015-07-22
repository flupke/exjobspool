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
      spawn fn ->
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
end
