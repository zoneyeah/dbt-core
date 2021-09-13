use crate::calculate::Calculation;
use crate::exceptions::{
    CalculateError,
    IOError,
};
use chrono::prelude::*;
use plotters::prelude::*;
use std::fs;
use std::fs::DirEntry;
use std::path::{Path, PathBuf};

pub fn draw_plot() -> Result<(), Box<dyn std::error::Error>> {
    let data = read_data(Path::new("plots/raw_data/"))?
        .into_iter()
        .map(|(datetime, calc)| (datetime, calc.data.difference));


    let root = BitMapBackend::new("plots/0.png", (640, 480)).into_drawing_area();
    root.fill(&WHITE)?;
    let mut chart = ChartBuilder::on(&root)
        .caption("y=x^2", ("sans-serif", 50).into_font())
        .margin(5)
        .x_label_area_size(30)
        .y_label_area_size(30)
        .build_cartesian_2d(-1f32..1f32, -0.1f32..1f32)?;

    chart.configure_mesh().draw()?;

    chart
        .draw_series(LineSeries::new(
            (date, data),
            &RED,
        ))?
        .label("y = x^2")
        .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], &RED));

    chart
        .configure_series_labels()
        .background_style(&WHITE.mix(0.8))
        .border_style(&BLACK)
        .draw()?;

    Ok(())
}

fn read_data(results_directory: &Path) -> Result<Vec<(DateTime<Utc>, Calculation)>, CalculateError> {
    fs::read_dir(results_directory)
        .or_else(|e| Err(IOError::ReadErr(results_directory.to_path_buf(), Some(e))))
        .or_else(|e| Err(CalculateError::CalculateIOError(e)))?
        .into_iter()
        .map(|entry| {
            let ent: DirEntry = entry
                .or_else(|e| Err(IOError::ReadErr(results_directory.to_path_buf(), Some(e))))
                .or_else(|e| Err(CalculateError::CalculateIOError(e)))?;

            Ok(ent.path())
        })
        .collect::<Result<Vec<PathBuf>, CalculateError>>()?
        .iter()
        .filter(|path| {
            path.extension()
                .and_then(|ext| ext.to_str())
                .map_or(false, |ext| ext.ends_with("json"))
        })
        .map(|path| {
            fs::read_to_string(path)
                .or_else(|e| Err(IOError::BadFileContentsErr(path.clone(), Some(e))))
                .or_else(|e| Err(CalculateError::CalculateIOError(e)))
                .and_then(|contents| {
                    let date = DateTime::<Utc>::tryFrom(path.file_name().unwrap())?;
                    let calc = serde_json::from_str::<Calculation>(&contents)
                        .or_else(|e| Err(CalculateError::BadJSONErr(path.clone(), Some(e))))?;
                    (date, calc)
                })
                .map(|m| (path.clone(), m))
        })
        .collect()
}
