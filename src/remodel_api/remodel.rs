use std::{
    collections::HashMap,
    ffi::OsStr,
    fs::{self, File},
    io::{BufReader, BufWriter},
    path::Path,
    sync::Arc,
    time::Duration,
};

use serde_with::{serde_as, Bytes};
use tokio;

use mlua::{Lua, LuaSerdeExt, ToLua, UserData, UserDataMethods};
use rbx_dom_weak::{types::VariantType, InstanceBuilder, WeakDom};
use reqwest::{
    header::{ACCEPT, CONTENT_TYPE, COOKIE, USER_AGENT},
    Method, StatusCode,
};

use crate::{
    lua_runtime::Runtime,
    remodel_context::RemodelContext,
    roblox_api::LuaInstance,
    sniff_type::{sniff_type, DocumentType},
    value::{lua_to_rbxvalue, rbxvalue_to_lua, type_from_str},
};

fn xml_encode_options() -> rbx_xml::EncodeOptions {
    rbx_xml::EncodeOptions::new().property_behavior(rbx_xml::EncodePropertyBehavior::WriteUnknown)
}

fn xml_decode_options() -> rbx_xml::DecodeOptions {
    rbx_xml::DecodeOptions::new().property_behavior(rbx_xml::DecodePropertyBehavior::ReadUnknown)
}

#[serde_as]
#[derive(serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct LuaHttpRequest {
    url: String,
    method: Option<String>,

    #[serde_as(as = "Option<HashMap<Bytes, Bytes>>")]
    headers: Option<HashMap<Vec<u8>, Vec<u8>>>,

    #[serde_as(as = "Option<Bytes>")]
    body: Option<Vec<u8>>,

    roblox_auth: Option<bool>,
    roblox_api_key: Option<bool>,
    roblox_csrf: Option<bool>,
}

#[serde_as]
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct LuaHttpResponse {
    success: bool,
    status_code: u16,
    status_message: String,
    #[serde_as(as = "HashMap<Bytes, Bytes>")]
    headers: HashMap<Vec<u8>, Vec<u8>>,
    #[serde_as(as = "Option<Bytes>")]
    body: Option<Vec<u8>>,
}

pub enum UploadPlaceAsset {
    Legacy(u64),
    CloudAPI { place_id: u64, universe_id: u64 },
}

pub struct Remodel;

impl Remodel {
    async fn read_xml_place_file<'lua>(
        context: &'lua Lua,
        path: &Path,
    ) -> mlua::Result<LuaInstance> {
        let path = path.to_owned();
        let source_tree = tokio::task::spawn_blocking(move || {
            let file = BufReader::new(File::open(path).map_err(mlua::Error::external)?);
            rbx_xml::from_reader(file, xml_decode_options()).map_err(mlua::Error::external)
        })
        .await
        .map_err(mlua::Error::external)??;

        Remodel::import_tree_root(context, source_tree)
    }

    async fn read_xml_model_file<'lua>(
        context: &'lua Lua,
        path: &Path,
    ) -> mlua::Result<Vec<LuaInstance>> {
        let path = path.to_owned();
        let source_tree = tokio::task::spawn_blocking(move || {
            let file = BufReader::new(File::open(path).map_err(mlua::Error::external)?);
            rbx_xml::from_reader(file, xml_decode_options()).map_err(mlua::Error::external)
        })
        .await
        .map_err(mlua::Error::external)??;

        Remodel::import_tree_children(context, source_tree)
    }

    async fn read_binary_place_file<'lua>(
        context: &'lua Lua,
        path: &Path,
    ) -> mlua::Result<LuaInstance> {
        let path = path.to_owned();
        let source_tree = tokio::task::spawn_blocking(move || {
            let file = BufReader::new(File::open(path).map_err(mlua::Error::external)?);
            rbx_binary::from_reader(file).map_err(mlua::Error::external)
        })
        .await
        .map_err(mlua::Error::external)??;

        Remodel::import_tree_root(context, source_tree)
    }

    async fn read_binary_model_file<'lua>(
        context: &'lua Lua,
        path: &Path,
    ) -> mlua::Result<Vec<LuaInstance>> {
        let path = path.to_owned();
        let source_tree = tokio::task::spawn_blocking(move || {
            let file = BufReader::new(File::open(path).map_err(mlua::Error::external)?);
            rbx_binary::from_reader(file).map_err(mlua::Error::external)
        })
        .await
        .map_err(mlua::Error::external)??;

        Remodel::import_tree_children(context, source_tree)
    }

    pub fn import_tree_children(
        context: &Lua,
        mut source_tree: WeakDom,
    ) -> mlua::Result<Vec<LuaInstance>> {
        let master_tree = RemodelContext::get(context)?.master_tree;
        let mut master_handle = master_tree.lock().unwrap();

        let source_root_ref = source_tree.root_ref();
        let source_root = source_tree.get_by_ref(source_root_ref).unwrap();
        let source_children = source_root.children().to_vec();

        let master_root_ref = master_handle.root_ref();

        let instances = source_children
            .into_iter()
            .map(|id| {
                source_tree.transfer(id, &mut master_handle, master_root_ref);
                LuaInstance::new(Arc::clone(&master_tree), id)
            })
            .collect::<Vec<_>>();

        Ok(instances)
    }

    pub fn import_tree_root(context: &Lua, mut source_tree: WeakDom) -> mlua::Result<LuaInstance> {
        let master_tree = RemodelContext::get(context)?.master_tree;
        let mut master_handle = master_tree.lock().unwrap();

        let source_children = source_tree.root().children().to_vec();
        let source_builder = InstanceBuilder::new("DataModel");

        let master_root_ref = master_handle.root_ref();
        let new_root_ref = master_handle.insert(master_root_ref, source_builder);

        for child_id in source_children {
            source_tree.transfer(child_id, &mut master_handle, new_root_ref);
        }

        Ok(LuaInstance::new(Arc::clone(&master_tree), new_root_ref))
    }

    async fn write_xml_place_file(lua_instance: LuaInstance, path: &Path) -> mlua::Result<()> {
        let path = path.to_owned();
        tokio::task::spawn_blocking(move || {
            let file = BufWriter::new(File::create(&path).map_err(mlua::Error::external)?);

            let tree = lua_instance.tree.lock().unwrap();
            let instance = tree
                .get_by_ref(lua_instance.id)
                .ok_or_else(|| mlua::Error::external("Cannot save a destroyed instance."))?;

            if instance.class != "DataModel" {
                return Err(mlua::Error::external(
                    "Only DataModel instances can be saved as place files.",
                ));
            }

            rbx_xml::to_writer(file, &tree, instance.children(), xml_encode_options())
                .map_err(mlua::Error::external)
        })
        .await
        .map_err(mlua::Error::external)?
    }

    async fn write_binary_place_file(lua_instance: LuaInstance, path: &Path) -> mlua::Result<()> {
        let path = path.to_owned();
        tokio::task::spawn_blocking(move || {
            let file = BufWriter::new(File::create(&path).map_err(mlua::Error::external)?);

            let tree = lua_instance.tree.lock().unwrap();
            let instance = tree
                .get_by_ref(lua_instance.id)
                .ok_or_else(|| mlua::Error::external("Cannot save a destroyed instance."))?;

            if instance.class != "DataModel" {
                return Err(mlua::Error::external(
                    "Only DataModel instances can be saved as place files.",
                ));
            }

            rbx_binary::to_writer(file, &tree, instance.children()).map_err(mlua::Error::external)
        })
        .await
        .map_err(mlua::Error::external)?
    }

    async fn write_xml_model_file(lua_instance: LuaInstance, path: &Path) -> mlua::Result<()> {
        let path = path.to_owned();
        tokio::task::spawn_blocking(move || {
            let file = BufWriter::new(File::create(&path).map_err(mlua::Error::external)?);

            let tree = lua_instance.tree.lock().unwrap();
            let instance = tree
                .get_by_ref(lua_instance.id)
                .ok_or_else(|| mlua::Error::external("Cannot save a destroyed instance."))?;

            if instance.class == "DataModel" {
                return Err(mlua::Error::external(
                    "DataModel instances must be saved as place files, not model files.",
                ));
            }

            rbx_xml::to_writer(file, &tree, &[lua_instance.id], xml_encode_options())
                .map_err(mlua::Error::external)
        })
        .await
        .map_err(mlua::Error::external)?
    }

    async fn write_binary_model_file(lua_instance: LuaInstance, path: &Path) -> mlua::Result<()> {
        let path = path.to_owned();
        tokio::task::spawn_blocking(move || {
            let file = BufWriter::new(File::create(&path).map_err(mlua::Error::external)?);

            let tree = lua_instance.tree.lock().unwrap();
            let instance = tree
                .get_by_ref(lua_instance.id)
                .ok_or_else(|| mlua::Error::external("Cannot save a destroyed instance."))?;

            if instance.class == "DataModel" {
                return Err(mlua::Error::external(
                    "DataModel instances must be saved as place files, not model files.",
                ));
            }

            rbx_binary::to_writer(file, &tree, &[lua_instance.id])
                .map_err(|err| mlua::Error::external(format!("{:?}", err)))
        })
        .await
        .map_err(mlua::Error::external)?
    }

    async fn read_model_asset(context: &Lua, asset_id: u64) -> mlua::Result<Vec<LuaInstance>> {
        let re_context = RemodelContext::get(context)?;
        let auth_cookie = re_context.auth_cookie();
        let url = format!("https://assetdelivery.roblox.com/v1/asset/?id={}", asset_id);

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(60 * 3))
            .build()
            .map_err(mlua::Error::external)?;

        let mut request = client.get(&url);

        if let Some(auth_cookie) = auth_cookie {
            request = request.header(COOKIE, format!(".ROBLOSECURITY={}", auth_cookie));
        } else {
            log::warn!("No auth cookie detected, Remodel may be unable to download this asset.");
        }

        let response = request.send().await.map_err(mlua::Error::external)?;

        let body: Vec<u8> = response
            .bytes()
            .await
            .map_err(mlua::Error::external)?
            .into();

        let source_tree = tokio::task::spawn_blocking(move || match sniff_type(&body) {
            Some(DocumentType::Binary) => {
                rbx_binary::from_reader(body.as_slice()).map_err(mlua::Error::external)
            }

            Some(DocumentType::Xml) => rbx_xml::from_reader(body.as_slice(), xml_decode_options())
                .map_err(mlua::Error::external),

            None => {
                let snippet = std::str::from_utf8(body.as_slice());

                let message = format!(
                    "Unknown response trying to read model asset ID {}. Response is:\n{:?}",
                    asset_id, snippet
                );

                return Err(mlua::Error::external(message));
            }
        })
        .await
        .map_err(mlua::Error::external)??;

        Remodel::import_tree_children(context, source_tree)
    }

    async fn read_place_asset(context: &Lua, asset_id: u64) -> mlua::Result<LuaInstance> {
        let re_context = RemodelContext::get(context)?;
        let auth_cookie = re_context.auth_cookie();
        let url = format!("https://assetdelivery.roblox.com/v1/asset/?id={}", asset_id);

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(60 * 3))
            .build()
            .map_err(mlua::Error::external)?;
        let mut request = client.get(&url);

        if let Some(auth_cookie) = auth_cookie {
            request = request.header(COOKIE, format!(".ROBLOSECURITY={}", auth_cookie));
        } else {
            log::warn!("No auth cookie detected, Remodel may be unable to download this asset.");
        }

        let response = request.send().await.map_err(mlua::Error::external)?;

        let body: Vec<u8> = response
            .bytes()
            .await
            .map_err(mlua::Error::external)?
            .into();

        let source_tree = tokio::task::spawn_blocking(move || match sniff_type(&body) {
            Some(DocumentType::Binary) => {
                rbx_binary::from_reader(body.as_slice()).map_err(mlua::Error::external)
            }

            Some(DocumentType::Xml) => rbx_xml::from_reader(body.as_slice(), xml_decode_options())
                .map_err(mlua::Error::external),

            None => {
                let snippet = std::str::from_utf8(body.as_slice());

                let message = format!(
                    "Unknown response trying to read model asset ID {}. Response is:\n{:?}",
                    asset_id, snippet
                );

                return Err(mlua::Error::external(message));
            }
        })
        .await
        .map_err(mlua::Error::external)??;

        Remodel::import_tree_root(context, source_tree)
    }

    async fn write_existing_model_asset(
        context: &Lua,
        lua_instance: LuaInstance,
        asset_id: u64,
    ) -> mlua::Result<()> {
        let buffer = tokio::task::spawn_blocking(move || {
            let tree = lua_instance.tree.lock().unwrap();
            let instance = tree
                .get_by_ref(lua_instance.id)
                .ok_or_else(|| mlua::Error::external("Cannot save a destroyed instance."))?;

            if instance.class == "DataModel" {
                return Err(mlua::Error::external(
                    "DataModel instances must be saved as place files, not model files.",
                ));
            }

            let mut buffer = Vec::new();
            rbx_binary::to_writer(&mut buffer, &tree, &[lua_instance.id])
                .map_err(mlua::Error::external)?;

            Ok(buffer)
        })
        .await
        .map_err(mlua::Error::external)??;

        Remodel::upload_asset(context, buffer, asset_id).await
    }

    async fn write_existing_place_asset(
        context: &Lua,
        lua_instance: LuaInstance,
        asset: UploadPlaceAsset,
    ) -> mlua::Result<()> {
        let buffer = tokio::task::spawn_blocking(move || {
            let tree = lua_instance.tree.lock().unwrap();
            let instance = tree
                .get_by_ref(lua_instance.id)
                .ok_or_else(|| mlua::Error::external("Cannot save a destroyed instance."))?;

            if instance.class != "DataModel" {
                return Err(mlua::Error::external(
                    "Only DataModel instances can be saved as place files.",
                ));
            }

            let mut buffer = Vec::new();
            rbx_binary::to_writer(&mut buffer, &tree, instance.children())
                .map_err(mlua::Error::external)?;

            Ok(buffer)
        })
        .await
        .map_err(mlua::Error::external)??;

        match asset {
            UploadPlaceAsset::Legacy(asset_id) => {
                Remodel::upload_asset(context, buffer, asset_id).await
            }
            UploadPlaceAsset::CloudAPI {
                place_id,
                universe_id,
            } => Remodel::cloud_upload_place_asset(context, buffer, universe_id, place_id).await,
        }
    }

    async fn cloud_upload_place_asset(
        context: &Lua,
        buffer: Vec<u8>,
        universe_id: u64,
        asset_id: u64,
    ) -> mlua::Result<()> {
        let re_context = RemodelContext::get(context)?;
        let url = format!(
            "https://apis.roblox.com/universes/v1/{}/places/{}/versions?versionType=Published",
            universe_id, asset_id
        );

        let api_key = re_context.api_key().ok_or_else(|| {
            mlua::Error::external(
                "Uploading a place asset using the Cloud API requires an auth key be set via --api-key or REMODEL_AUTH_KEY environment variable",
            )
        })?;

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(60 * 3))
            .build()
            .map_err(mlua::Error::external)?;

        let build_request = move || {
            client
                .post(&url)
                .header("x-api-key", api_key)
                .header(CONTENT_TYPE, "application/xml")
                .header(ACCEPT, "application/json")
                .body(buffer.clone())
        };

        log::debug!("Uploading to Roblox Cloud...");
        let response = build_request()
            .send()
            .await
            .map_err(mlua::Error::external)?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(mlua::Error::external(format!(
                "Roblox API returned an error, status {}.",
                response.status()
            )))
        }
    }

    async fn upload_asset(context: &Lua, buffer: Vec<u8>, asset_id: u64) -> mlua::Result<()> {
        let re_context = RemodelContext::get(context)?;
        let auth_cookie = re_context.auth_cookie().ok_or_else(|| {
            mlua::Error::external(
                "Uploading assets requires an auth cookie, please log into Roblox Studio.",
            )
        })?;

        let url = format!(
            "https://data.roblox.com/Data/Upload.ashx?assetid={}",
            asset_id
        );

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(60 * 3))
            .build()
            .map_err(mlua::Error::external)?;

        let build_request = move || {
            client
                .post(&url)
                .header(COOKIE, format!(".ROBLOSECURITY={}", auth_cookie))
                .header(USER_AGENT, "Roblox/WinInet")
                .header(CONTENT_TYPE, "application/xml")
                .header(ACCEPT, "application/json")
                .body(buffer.clone())
        };

        log::debug!("Uploading to Roblox...");
        let mut response = build_request()
            .send()
            .await
            .map_err(mlua::Error::external)?;

        // Starting in Feburary, 2021, the upload endpoint performs CSRF challenges.
        // If we receive an HTTP 403 with a X-CSRF-Token reply, we should retry the
        // request, echoing the value of that header.
        if response.status() == StatusCode::FORBIDDEN {
            if let Some(csrf_token) = response.headers().get("X-CSRF-Token") {
                log::debug!("Received CSRF challenge, retrying with token...");
                response = build_request()
                    .header("X-CSRF-Token", csrf_token)
                    .send()
                    .await
                    .map_err(mlua::Error::external)?;
            }
        }

        if response.status().is_success() {
            Ok(())
        } else {
            Err(mlua::Error::external(format!(
                "Roblox API returned an error, status {}.",
                response.status()
            )))
        }
    }

    async fn http_request(
        context: &Lua,
        options: &LuaHttpRequest,
    ) -> mlua::Result<LuaHttpResponse> {
        let re_context = RemodelContext::get(context)?;

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(60 * 3))
            .build()
            .map_err(mlua::Error::external)?;

        let method = options.method.as_ref().map_or_else(|| "GET", |v| &v);
        let method = Method::try_from(method)
            .map_err(|_| mlua::Error::external(format!("Invalid HTTP method: {}", method)))?;

        let mut request = client.request(method, &options.url);

        if let Some(true) = options.roblox_auth {
            let auth_cookie = re_context.auth_cookie().ok_or_else(|| {
                mlua::Error::external(
                    "HTTP request attempted with RobloxAuth = true, but you're not signed in to Studio.",
                )
            })?;

            request = request.header(COOKIE, format!(".ROBLOSECURITY={}", auth_cookie));
        }

        if let Some(true) = options.roblox_api_key {
            let api_key = re_context.api_key().ok_or_else(|| {
                mlua::Error::external(
                    "HTTP request attempted with RobloxApiKey = true, but an API key was not provided.",
                )
            })?;

            request = request.header("x-api-key", api_key);
        }

        if let Some(headers) = &options.headers {
            for (key, value) in headers.iter() {
                request = request.header(&key[..], &value[..]);
            }
        }

        if let Some(body) = &options.body {
            request = request.body(body.clone());
        }

        let base_request = request.try_clone().unwrap();

        let mut response = request.send().await.map_err(mlua::Error::external)?;

        if let Some(true) = options.roblox_csrf {
            if response.status() == StatusCode::FORBIDDEN {
                if let Some(csrf_token) = response.headers().get("X-CSRF-Token") {
                    log::debug!("Received CSRF challenge, retrying with token...");
                    response = base_request
                        .header("X-CSRF-Token", csrf_token)
                        .send()
                        .await
                        .map_err(mlua::Error::external)?;
                }
            }
        }

        let status = response.status();

        let mut headers = HashMap::new();
        for (key, value) in response.headers().iter() {
            headers.insert(key.as_str().as_bytes().to_vec(), value.as_bytes().to_vec());
        }

        let body = response.bytes().await.map_err(mlua::Error::external)?;

        Ok(LuaHttpResponse {
            success: status.is_success(),
            status_code: status.as_u16(),
            status_message: status
                .canonical_reason()
                .unwrap_or_else(|| status.as_str())
                .to_string(),
            headers,
            body: Some(body.to_vec()),
        })
    }

    fn get_raw_property<'a>(
        context: &'a Lua,
        lua_instance: LuaInstance,
        name: &str,
    ) -> mlua::Result<mlua::Value<'a>> {
        let tree = lua_instance.tree.lock().unwrap();

        let instance = tree.get_by_ref(lua_instance.id).ok_or_else(|| {
            mlua::Error::external("Cannot call remodel.getRawProperty on a destroyed instance.")
        })?;

        match instance.properties.get(name) {
            Some(value) => {
                rbxvalue_to_lua(context, value, Some((&tree, lua_instance.tree.clone())))
            }
            None => Ok(mlua::Value::Nil),
        }
    }

    fn set_raw_property(
        lua_instance: LuaInstance,
        key: String,
        ty: VariantType,
        lua_value: mlua::Value<'_>,
    ) -> mlua::Result<()> {
        let mut tree = lua_instance.tree.lock().unwrap();

        let instance = tree.get_by_ref_mut(lua_instance.id).ok_or_else(|| {
            mlua::Error::external("Cannot call remodel.setRawProperty on a destroyed instance.")
        })?;

        let value = lua_to_rbxvalue(ty, lua_value)?;
        instance.properties.insert(key, value);

        Ok(())
    }

    async fn run_command<'a>(
        context: &'a Lua,
        command: &str,
        args: Vec<&str>,
    ) -> mlua::Result<mlua::Value<'a>> {
        let result = tokio::process::Command::new(command)
            .args(args)
            .output()
            .await
            .map_err(|err| {
                mlua::Error::external(format!("Failed to run command: {}", err.to_string()))
            })?;

        let mut results_table = HashMap::new();
        results_table.insert(
            "stdout",
            mlua::Value::String(context.create_string(&result.stdout)?),
        );
        results_table.insert(
            "stderr",
            mlua::Value::String(context.create_string(&result.stderr)?),
        );
        results_table.insert("code", result.status.code().to_lua(context)?);
        results_table.insert("success", result.status.success().to_lua(context)?);

        Ok(results_table.to_lua(context)?)
    }
}

impl UserData for Remodel {
    fn add_fields<'lua, F: mlua::UserDataFields<'lua, Self>>(fields: &mut F) {
        Runtime::userdata_async_fn_many(
            fields,
            vec![
                ("httpRequest", "remodel._httpRequest"),
                ("runCommand", "remodel._runCommand"),
                ("readPlaceFile", "remodel._readPlaceFile"),
                ("readModelFile", "remodel._readModelFile"),
                ("readModelAsset", "remodel._readModelAsset"),
                ("readPlaceAsset", "remodel._readPlaceAsset"),
                (
                    "writeExistingModelAsset",
                    "remodel._writeExistingModelAsset",
                ),
                (
                    "writeExistingPlaceAsset",
                    "remodel._writeExistingPlaceAsset",
                ),
                ("publishPlaceToUniverse", "remodel._publishPlaceToUniverse"),
                ("writePlaceFile", "remodel._writePlaceFile"),
                ("writeModelFile", "remodel._writeModelFile"),
            ],
        );
    }
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_function(
            "getRawProperty",
            |context, (instance, name): (LuaInstance, String)| {
                Self::get_raw_property(context, instance, &name)
            },
        );

        methods.add_function(
            "setRawProperty",
            |_context, (instance, name, lua_ty, value): (LuaInstance, String, String, mlua::Value<'_>)| {
                let ty = type_from_str(&lua_ty)
                    .ok_or_else(|| mlua::Error::external(format!("{} is not a valid Roblox type.", lua_ty)))?;

                Self::set_raw_property(instance, name, ty, value)
            },
        );

        methods.add_async_function("_httpRequest", |context, options: mlua::Value| async move {
            context.to_value(
                &Remodel::http_request(context, &context.from_value::<LuaHttpRequest>(options)?)
                    .await?,
            )
        });

        methods.add_async_function(
            "_runCommand",
            |context, (command, args): (String, Option<Vec<String>>)| async move {
                Remodel::run_command(
                    context,
                    &command,
                    args.unwrap_or_else(|| Vec::new())
                        .iter()
                        .map(|s| s.as_str())
                        .collect(),
                )
                .await
            },
        );

        methods.add_async_function("_readPlaceFile", |context, lua_path: String| async move {
            let path = Path::new(&lua_path);

            match path.extension().and_then(OsStr::to_str) {
                Some("rbxlx") => Remodel::read_xml_place_file(context, path).await,
                Some("rbxl") => Remodel::read_binary_place_file(context, path).await,
                _ => Err(mlua::Error::external(format!(
                    "Invalid place file path {}",
                    path.display()
                ))),
            }
        });

        methods.add_async_function("_readModelFile", |context, lua_path: String| async move {
            let path = Path::new(&lua_path);

            match path.extension().and_then(OsStr::to_str) {
                Some("rbxmx") => Remodel::read_xml_model_file(context, path).await,
                Some("rbxm") => Remodel::read_binary_model_file(context, path).await,
                _ => Err(mlua::Error::external(format!(
                    "Invalid model file path {}",
                    path.display()
                ))),
            }
        });

        methods.add_async_function("_readModelAsset", |context, asset_id: String| async move {
            let asset_id = asset_id.parse().map_err(mlua::Error::external)?;

            Remodel::read_model_asset(context, asset_id).await
        });

        methods.add_async_function("_readPlaceAsset", |context, asset_id: String| async move {
            let asset_id = asset_id.parse().map_err(mlua::Error::external)?;

            Remodel::read_place_asset(context, asset_id).await
        });

        methods.add_async_function(
            "_writeExistingModelAsset",
            |context, (instance, asset_id): (LuaInstance, String)| async move {
                let asset_id = asset_id.parse().map_err(mlua::Error::external)?;

                Remodel::write_existing_model_asset(context, instance, asset_id).await
            },
        );

        methods.add_async_function(
            "_writeExistingPlaceAsset",
            |context, (instance, asset_id): (LuaInstance, String)| async move {
                let asset_id = asset_id.parse().map_err(mlua::Error::external)?;

                Remodel::write_existing_place_asset(
                    context,
                    instance,
                    UploadPlaceAsset::Legacy(asset_id),
                )
                .await
            },
        );

        methods.add_async_function(
            "_publishPlaceToUniverse",
            |context, (instance, universe_id, place_id): (LuaInstance, u64, u64)| {
                Remodel::write_existing_place_asset(
                    context,
                    instance,
                    UploadPlaceAsset::CloudAPI {
                        universe_id,
                        place_id,
                    },
                )
            },
        );

        methods.add_async_function(
            "_writePlaceFile",
            |_context, (lua_path, instance): (String, LuaInstance)| async move {
                let path = Path::new(&lua_path);

                match path.extension().and_then(OsStr::to_str) {
                    Some("rbxlx") => Remodel::write_xml_place_file(instance, path).await,
                    Some("rbxl") => Remodel::write_binary_place_file(instance, path).await,
                    _ => Err(mlua::Error::external(format!(
                        "Invalid place file path {}",
                        path.display()
                    ))),
                }
            },
        );

        methods.add_async_function(
            "_writeModelFile",
            |_context, (lua_path, instance): (String, LuaInstance)| async move {
                let path = Path::new(&lua_path);

                match path.extension().and_then(OsStr::to_str) {
                    Some("rbxmx") => Remodel::write_xml_model_file(instance, path).await,
                    Some("rbxm") => Remodel::write_binary_model_file(instance, path).await,
                    _ => Err(mlua::Error::external(format!(
                        "Invalid model file path {}",
                        path.display()
                    ))),
                }
            },
        );

        methods.add_function("readFile", |_context, path: String| {
            fs::read_to_string(path).map_err(mlua::Error::external)
        });

        methods.add_function("readDir", |_context, path: String| {
            fs::read_dir(path)
                .map_err(mlua::Error::external)?
                .filter_map(|entry| {
                    let entry = match entry {
                        Ok(entry) => entry,
                        Err(err) => return Some(Err(mlua::Error::external(err))),
                    };

                    match entry.file_name().into_string() {
                        Ok(name) => Some(Ok(name)),
                        Err(bad_name) => {
                            log::warn!(
                                "Encountered invalid Unicode file name {:?}, skipping.",
                                bad_name
                            );
                            None
                        }
                    }
                })
                .collect::<Result<Vec<String>, _>>()
        });

        methods.add_function(
            "writeFile",
            |_context, (path, contents): (String, mlua::String)| {
                fs::write(path, contents.as_bytes()).map_err(mlua::Error::external)
            },
        );

        methods.add_function("createDirAll", |_context, path: String| {
            fs::create_dir_all(path).map_err(mlua::Error::external)
        });

        methods.add_function("isFile", |_context, path: String| {
            let meta = fs::metadata(path).map_err(mlua::Error::external)?;
            Ok(meta.is_file())
        });

        methods.add_function("isDir", |_context, path: String| {
            let meta = fs::metadata(path).map_err(mlua::Error::external)?;
            Ok(meta.is_dir())
        });

        methods.add_function("removeFile", |_context, path: String| {
            fs::remove_file(path).map_err(mlua::Error::external)
        });

        methods.add_function("removeDir", |_context, path: String| {
            fs::remove_dir_all(path).map_err(mlua::Error::external)
        });
    }
}
