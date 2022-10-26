local rust_async_tasks = {}

local function pack(...)
	return { n = select("#", ...), ... }
end

local function execute_rust_async(task, callback, ...)
	xpcall(function(task_inner, callback_inner, ...)
		task_inner.result = {
			Success = pack(callback_inner(...)),
		}
	end, function(err)
		err = tostring(err)
		if err:find("caused by:") then
			err = err:match("caused by: (.+)$")
		end

		task.result = {
			Error = err,
		}
	end, task, callback, ...)

	rust_async_tasks[task] = nil
	if not task.moved_on then
		coroutine.resume(task.owner_thread)
	end
end

local function spawn_rust(callback, ...)
	local task = {}
	task.owner_thread = coroutine.running()
	task.task_thread = coroutine.create(execute_rust_async)
	task.result = nil
	task.moved_on = false

	coroutine.resume(task.task_thread, task, callback, ...)

	if task.result == nil then
		rust_async_tasks[task] = true
		coroutine.yield()
	end

	task.moved_on = true
	if task.result then
		if task.result.Success then
			return unpack(task.result.Success, 1, task.result.Success.n)
		else
			error(task.result.Error, -1)
		end
	else
		return nil
	end
end

local lua_async_tasks = {}

local function execute_lua_async(callback, ...)
	local err, traceback
	xpcall(function(callback_inner, ...)
		callback_inner(...)
	end, function(err_inner)
		err = err_inner
		traceback = debug.traceback(nil, 2)
	end, callback, ...)
	lua_async_tasks[coroutine.running()] = nil

	if traceback then
		print(string.format("%s\nstack traceback:\n%s", tostring(err), "\t" .. traceback:gsub("\n", "\n\t")))
	end
end

local function spawn_lua(callback, ...)
	local thread
	if type(callback) == "thread" then
		thread = callback
		lua_async_tasks[thread] = true
		coroutine.resume(thread, ...)
	else
		thread = coroutine.create(execute_lua_async)
		lua_async_tasks[thread] = true
		coroutine.resume(thread, callback, ...)
	end

	return thread
end

local lua_async_tasks_deferred = {}

local function defer_lua(callback, ...)
	local thread
	if type(callback) == "thread" then
		thread = callback
		table.insert(lua_async_tasks_deferred, pack(thread, ...))
	else
		thread = coroutine.create(execute_lua_async)
		table.insert(lua_async_tasks_deferred, pack(thread, callback, ...))
	end

	return thread
end

local function debug_list_lua_tasks()
	for task, _ in lua_async_tasks do
		print(
			string.format(
				"lua task\n  %s\n  %s\n  %s",
				tostring(task),
				coroutine.status(task),
				(debug.traceback(task):gsub("\n", "\n  "))
			)
		)
	end
end

local function start()
	while true do
		for task, _ in rust_async_tasks do
			coroutine.resume(task.task_thread)
		end
		coroutine.yield()

		for _, task_args in lua_async_tasks_deferred do
			coroutine.resume(unpack(task_args, 1, task_args.n))
		end

		for task, _ in lua_async_tasks do
			if coroutine.status(task) == "dead" then
				lua_async_tasks[task] = nil
			end
		end

		if next(lua_async_tasks) == nil then
			break
		end
	end
end

local task_lib = {
	spawn = spawn_lua,
	defer = defer_lua,
}

function task_lib.wait(duration)
	local timer_start = os.clock()
	spawn_rust(function()
		repeat
			coroutine.yield()
		until os.clock() >= timer_start + (duration or 0)
	end)

	return os.clock() - timer_start
end

local function execute_delay(duration, callback, ...)
	task_lib.wait(duration)
	spawn_lua(callback, ...)
end

function task_lib.delay(duration, callback, ...)
	return spawn_lua(execute_delay, duration, callback, ...)
end


return {
	spawn_rust = spawn_rust,
	spawn_lua = spawn_lua,
	defer_lua = defer_lua,
	start = start,

	task_lib = task_lib,

	debug = {
		list_lua_tasks = debug_list_lua_tasks,
	},
}
