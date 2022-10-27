use mlua::{Function, Table, ToLuaMulti, UserData, UserDataFields};

pub struct Runtime {}

impl Runtime {
    pub fn inject_runtime<'lua>(lua: &'lua mlua::Lua) -> mlua::Result<()> {
        let runtime_source = include_str!("runtime.lua");

        let runtime_chunk = lua.load(runtime_source).set_name("_runtime")?;
        let runtime_lib = runtime_chunk.call::<_, mlua::Table>(())?;

        let task_lib: Table = runtime_lib.get("task_lib")?;

        lua.globals().set("_runtime", runtime_lib)?;
        lua.globals().set("task", task_lib)?;

        Ok(())
    }

    pub fn spawn_lua<'lua, A: ToLuaMulti<'lua>>(
        lua: &'lua mlua::Lua,
        func: Function<'lua>,
        args: A,
    ) -> mlua::Result<()> {
        let spawn_lua = lua
            .globals()
            .get::<_, mlua::Table>("_runtime")?
            .get::<_, mlua::Function>("spawn_lua")?;

        let mut args = args.to_lua_multi(lua)?.clone();
        args.push_front(mlua::Value::Function(func));

        spawn_lua.call::<_, ()>(args)?;

        Ok(())
    }

    pub async fn start<'lua>(lua: &'lua mlua::Lua) -> mlua::Result<()> {
        let start = lua
            .globals()
            .get::<_, mlua::Table>("_runtime")?
            .get::<_, mlua::Function>("start")?;

        start.call_async(()).await
    }

    pub fn userdata_async_fn<'lua, T, P>(
        fields: &mut P,
        public_func: &str,
        private_func: &str,
    ) -> ()
    where
        T: UserData + 'static,
        P: UserDataFields<'lua, T>,
    {
        let private_func = private_func.to_owned();
        fields.add_field_function_get(public_func, move |context, _| {
            let spawn_rust = context
                .globals()
                .get::<_, mlua::Table>("_runtime")?
                .get::<_, mlua::Function>("spawn_rust")?;

            let env = context.create_table()?;
            env.set("spawn_rust", spawn_rust)?;

            let meta = context.create_table()?;
            meta.set("__index", context.globals())?;
            env.set_metatable(Some(meta));

            return context
                .load(&format!("return spawn_rust({}, ...)", private_func))
                .set_name("_lua_runtime_wrapper")?
                .set_environment(env)?
                .into_function();
        });
    }

    pub fn userdata_async_fn_many<'lua, T, P>(fields: &mut P, funcs: Vec<(&str, &str)>) -> ()
    where
        T: UserData + 'static,
        P: UserDataFields<'lua, T>,
    {
        for (public_func, private_func) in funcs {
            Runtime::userdata_async_fn(fields, &public_func, &private_func);
        }
    }
}
