use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

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


/// This module is a python module implemented in Rust.
/// the function name must match the library name in Cargo.toml
#[pymodule]
fn tracking(_: Python, m: &PyModule) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(collector_url))?;
    m.add_wrapped(wrap_pyfunction!(collector_protocol))?;
    m.add_wrapped(wrap_pyfunction!(invocation_spec))?;
    m.add_wrapped(wrap_pyfunction!(platform_spec))?;
    m.add_wrapped(wrap_pyfunction!(run_model_spec))?;
    m.add_wrapped(wrap_pyfunction!(invocation_new_spec))?;
    m.add_wrapped(wrap_pyfunction!(package_install_spec))?;
    m.add_wrapped(wrap_pyfunction!(deprecation_warn_spec))?;
    m.add_wrapped(wrap_pyfunction!(load_all_timing_spec))?;
    m.add_wrapped(wrap_pyfunction!(dbt_invocation_env))?;
    Ok(())
}
