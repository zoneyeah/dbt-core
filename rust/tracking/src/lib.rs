use pyo3::prelude::*;

#[pyfunction]
pub fn collector_url() -> PyResult<String> { Ok("fishtownanalytics.sinter-collect.com".to_owned().to_owned()) }
#[pyfunction]
pub fn collector_protocol() -> PyResult<String> { Ok("https".to_owned()) }

#[pyfunction]
pub fn invocation_spec() -> PyResult<String> { Ok("iglu:com.dbt/invocation/jsonschema/1-0-1".to_owned()) }
#[pyfunction]
pub fn platform_spec() -> PyResult<String> { Ok("iglu:com.dbt/platform/jsonschema/1-0-0".to_owned()) }
#[pyfunction]
pub fn run_model_spec() -> PyResult<String> { Ok("iglu:com.dbt/run_model/jsonschema/1-0-1".to_owned()) }
#[pyfunction]
pub fn invocation_new_spec() -> PyResult<String> { Ok("iglu:com.dbt/invocation_env/jsonschema/1-0-0".to_owned()) }
#[pyfunction]
pub fn package_install_spec() -> PyResult<String> { Ok("iglu:com.dbt/package_install/jsonschema/1-0-0".to_owned()) }
#[pyfunction]
pub fn rpc_request_spec() -> PyResult<String> { Ok("iglu:com.dbt/rpc_request/jsonschema/1-0-1".to_owned()) }
#[pyfunction]
pub fn deprecation_warn_spec() -> PyResult<String> { Ok("iglu:com.dbt/deprecation_warn/jsonschema/1-0-0".to_owned()) }
#[pyfunction]
pub fn load_all_timing_spec() -> PyResult<String> { Ok("iglu:com.dbt/load_all_timing/jsonschema/1-0-0".to_owned()) }

#[pyfunction]
pub fn dbt_invocation_env() -> PyResult<String> { Ok("DBT_INVOCATION_ENV".to_owned()) }
