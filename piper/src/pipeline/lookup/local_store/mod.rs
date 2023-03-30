use std::{
    collections::{BTreeMap, HashMap},
    sync::RwLock,
};

use async_trait::async_trait;
use polars::{
    io::is_cloud_url,
    prelude::{cloud::CloudOptions, *},
};
use serde::{Deserialize, Serialize};
use tracing::{debug, instrument};

use crate::{pipeline::lookup::get_secret, LookupSource, PiperError, Value};

mod any_value;

use any_value::to_db_key;

#[derive(Copy, Clone, PartialEq, Eq, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum FileFormat {
    #[default]
    Auto,
    Csv,
    Parquet,
    Json,
    Ndjson,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalStoreSource {
    path: String,
    key_column: String,
    #[serde(default)]
    fields: Vec<String>,
    #[serde(default)]
    format: FileFormat,
    #[serde(default)]
    local_path: Option<String>,
    #[serde(default)]
    cloud_config: HashMap<String, String>,
    #[serde(skip)]
    db: Option<Arc<RwLock<BTreeMap<String, Value>>>>,
}

impl LocalStoreSource {
    pub fn new(
        path: String,
        key_column: String,
        fields: Vec<String>,
        format: FileFormat,
        local_path: Option<String>,
        cloud_config: HashMap<String, String>,
    ) -> Result<Self, PiperError> {
        let mut s = Self {
            path,
            key_column,
            fields,
            format,
            local_path,
            cloud_config,
            db: None,
        };
        s.load_db()?;
        Ok(s)
    }

    fn load_db(&mut self) -> Result<(), PiperError> {
        let path = get_secret(Some(&self.path))?;
        let df = if is_cloud_url(&path) {
            let mut args = ScanArgsParquet::default();
            let mut options: Vec<(String, String)> = Vec::new();
            for (k, v) in self.cloud_config.iter() {
                options.push((k.to_string(), get_secret(Some(v))?));
            }
            let options = CloudOptions::from_untyped_config(&path, options)
                .map_err(|e| PiperError::ExternalError(e.to_string()))?;
            args.cloud_options = Some(options);
            match get_file_format(&path, self.format)? {
                FileFormat::Parquet => LazyFrame::scan_parquet(&path, args)
                    .map_err(|e| PiperError::ExternalError(e.to_string()))?
                    .collect()
                    .map_err(|e| PiperError::ExternalError(e.to_string()))?,
                _ => {
                    return Err(PiperError::ExternalError(format!(
                        "Unsupported file format for file {}",
                        path
                    )))
                }
            }
        } else {
            match get_file_format(&path, self.format)? {
                FileFormat::Csv => CsvReader::from_path(&path)
                    .map_err(|e| PiperError::ExternalError(e.to_string()))?
                    .has_header(true)
                    .infer_schema(Some(100))
                    .finish()
                    .map_err(|e| PiperError::ExternalError(e.to_string()))?,
                FileFormat::Parquet => {
                    let mut file = std::fs::File::open(&path)
                        .map_err(|e| PiperError::ExternalError(e.to_string()))?;
                    ParquetReader::new(&mut file)
                        .finish()
                        .map_err(|e| PiperError::ExternalError(e.to_string()))?
                }
                FileFormat::Json => {
                    let mut file = std::fs::File::open(&path)
                        .map_err(|e| PiperError::ExternalError(e.to_string()))?;
                    JsonReader::new(&mut file)
                        .finish()
                        .map_err(|e| PiperError::ExternalError(e.to_string()))?
                }
                FileFormat::Ndjson => {
                    let mut file = std::fs::File::open(&path)
                        .map_err(|e| PiperError::ExternalError(e.to_string()))?;
                    JsonLineReader::new(&mut file)
                        .finish()
                        .map_err(|e| PiperError::ExternalError(e.to_string()))?
                }
                _ => {
                    return Err(PiperError::ExternalError(format!(
                        "Unsupported file format for file {}",
                        path
                    )))
                }
            }
        };

        let db = Arc::new(RwLock::new(BTreeMap::new()));

        let keys: Vec<String> = df
            .column(&self.key_column)
            .map_err(|e| PiperError::ExternalError(e.to_string()))?
            .iter()
            .map(|v| to_db_key(&v))
            .collect();

        let fields = if self.fields.is_empty() {
            df.get_column_names()
                .into_iter()
                .map(|s| s.to_string())
                .collect()
        } else {
            self.fields.to_vec()
        };

        let mut writer = db
            .write()
            .map_err(|e| PiperError::ExternalError(e.to_string()))?;

        for f in &fields {
            debug!("Loading field {}", f);
            let col = df
                .column(f)
                .map_err(|e| PiperError::ExternalError(e.to_string()))?;
            let i = keys.iter().zip(col.iter());
            for (k, v) in i {
                let key = format!("{}\0{}", f, k);
                writer.insert(key, v.into());
            }
        }

        self.fields = fields;
        self.db = Some(db.clone());
        Ok(())
    }

    async fn do_lookup(&self, k: &Value, fields: &[String]) -> Result<Vec<Vec<Value>>, PiperError> {
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| PiperError::ExternalError("Database not initialized".to_string()))?
            .read()
            .map_err(|e| PiperError::ExternalError(e.to_string()))?;
        let mut result = Vec::new();
        let k = to_db_key(k);
        for f in fields {
            let key = format!("{}\0{}", f, k);
            let value = db.get(&key).cloned().unwrap_or_default();
            result.push(value);
        }
        Ok(vec![result])
    }
}

#[async_trait]
impl LookupSource for LocalStoreSource {
    fn init(&mut self) -> Result<(), PiperError> {
        let s = &mut Self::new(
            self.path.clone(),
            self.key_column.clone(),
            self.fields.clone(),
            self.format,
            self.local_path.clone(),
            Default::default(),
        )?;
        self.fields = s.fields.clone();
        self.db = s.db.clone();
        Ok(())
    }

    #[instrument(level = "trace", skip(self))]
    async fn lookup(&self, k: &Value, fields: &[String]) -> Vec<Value> {
        let ret = self.do_lookup(k, fields).await;
        match ret {
            Ok(v) => v
                .get(0)
                .cloned()
                .unwrap_or_else(|| vec![Value::Null; fields.len()]),
            Err(e) => {
                vec![e.into(); fields.len()]
            }
        }
    }

    #[instrument(level = "trace", skip(self))]
    async fn join(&self, k: &Value, fields: &[String]) -> Vec<Vec<Value>> {
        let ret = self.do_lookup(k, fields).await;
        match ret {
            Ok(v) => v,
            Err(e) => {
                vec![vec![e.into(); fields.len()]]
            }
        }
    }

    fn dump(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap()
    }

    fn batch_size(&self) -> usize {
        super::DEFAULT_CONCURRENCY
    }
}

fn get_file_format(path: &str, format: FileFormat) -> Result<FileFormat, PiperError> {
    if format != FileFormat::Auto {
        return Ok(format);
    }
    if path.ends_with(".csv") || path.ends_with(".csv.gz") {
        Ok(FileFormat::Csv)
    } else if path.ends_with(".parquet") {
        Ok(FileFormat::Parquet)
    } else if path.ends_with(".json") || path.ends_with(".json.gz") {
        Ok(FileFormat::Json)
    } else if path.ends_with(".ndjson") || path.ends_with(".ndjson.gz") {
        Ok(FileFormat::Ndjson)
    } else {
        Err(PiperError::ExternalError(format!(
            "Unsupported file format for file {}",
            path
        )))
    }
}

#[cfg(test)]
mod tests {
    use tracing::debug;

    use crate::IntoValue;

    use super::*;

    fn fix_current_dir() {
        if ! std::path::Path::new("test-data").exists() {
            std::env::set_current_dir("..").unwrap();
        }
        debug!("Current dir: {}", std::env::current_dir().unwrap().display());
    }

    #[tokio::test]
    async fn test_load_parquet() {
        fix_current_dir();
        let path = "test-data/links.parquet";
        let key_column = "movieId";
        let fields = vec!["imdbId".to_string(), "tmdbId".to_string()];
        let format = FileFormat::Auto;
        let local_path = None;
        let src = LocalStoreSource::new(
            path.to_string(),
            key_column.to_string(),
            fields.clone(),
            format,
            local_path,
            Default::default(),
        )
        .unwrap();
        let r = src
            .lookup(
                &Value::Int(1),
                &["imdbId".to_string(), "tmdbId".to_string()],
            )
            .await;
        assert_eq!(r[0], Value::Int(114709));
        assert_eq!(r[1], Value::Int(862));
        let r = src
            .lookup(
                &Value::Int(6),
                &["imdbId".to_string(), "tmdbId".to_string()],
            )
            .await;
        assert_eq!(r[0], Value::Int(113277));
        assert_eq!(r[1], Value::Int(949));
    }

    #[tokio::test]
    async fn test_load_csv() {
        fix_current_dir();
        let path = "test-data/test.csv";
        let key_column = "C1";
        let fields = vec!["C2".to_string(), "C3".to_string()];
        let format = FileFormat::Auto;
        let local_path = None;
        let src = LocalStoreSource::new(
            path.to_string(),
            key_column.to_string(),
            fields.clone(),
            format,
            local_path,
            Default::default(),
        )
        .unwrap();
        let r = src
            .lookup(
                &Value::Int(1),
                &["C2".to_string(), "C3".to_string()],
            )
            .await;
        assert_eq!(r[0], "AaA".into_value());
        assert_eq!(r[1], "BbB".into_value());
        let r = src
            .lookup(
                &Value::Int(3),
                &["C2".to_string(), "C3".to_string()],
            )
            .await;
        assert_eq!(r[0], "EeE".into_value());
        assert_eq!(r[1], "FfF".into_value());
    }

    #[tokio::test]
    async fn test_load_json() {
        fix_current_dir();
        let path = "test-data/test.json";
        let key_column = "C1";
        let fields = vec!["C2".to_string(), "C3".to_string()];
        let format = FileFormat::Auto;
        let local_path = None;
        let src = LocalStoreSource::new(
            path.to_string(),
            key_column.to_string(),
            fields.clone(),
            format,
            local_path,
            Default::default(),
        )
        .unwrap();
        let r = src
            .lookup(
                &Value::Int(1),
                &["C2".to_string(), "C3".to_string()],
            )
            .await;
        assert_eq!(r[0], "AaA".into_value());
        assert_eq!(r[1], "BbB".into_value());
        let r = src
            .lookup(
                &Value::Int(3),
                &["C2".to_string(), "C3".to_string()],
            )
            .await;
        assert_eq!(r[0], "EeE".into_value());
        assert_eq!(r[1], "FfF".into_value());
    }

    #[tokio::test]
    async fn test_load_ndjson() {
        fix_current_dir();
        let path = "test-data/test_nd.json";
        let key_column = "C1";
        let fields = vec!["C2".to_string(), "C3".to_string()];
        let format = FileFormat::Ndjson;
        let local_path = None;
        let src = LocalStoreSource::new(
            path.to_string(),
            key_column.to_string(),
            fields.clone(),
            format,
            local_path,
            Default::default(),
        )
        .unwrap();
        let r = src
            .lookup(
                &Value::Int(1),
                &["C2".to_string(), "C3".to_string()],
            )
            .await;
        assert_eq!(r[0], "AaA".into_value());
        assert_eq!(r[1], "BbB".into_value());
        let r = src
            .lookup(
                &Value::Int(3),
                &["C2".to_string(), "C3".to_string()],
            )
            .await;
        assert_eq!(r[0], "EeE".into_value());
        assert_eq!(r[1], "FfF".into_value());
    }

    /// An upstream bug of pola-rs currently prevents reading from Azure Data Lake Gen2
    /// Disable this test until the bug is fixed.
    /// @see https://github.com/pola-rs/polars/issues/3906
    #[tokio::test]
    #[ignore]
    async fn test_load_cloud() {
        dotenvy::dotenv().ok();
        let path = "abfs://xchfeathrtest4fs@xchfeathrtest4sto.blob.core.windows.net/links.parquet";
        let key_column = "movieId";
        let fields = vec!["imdbId".to_string(), "tmdbId".to_string()];
        let format = FileFormat::Auto;
        let local_path = None;
        let mut cloud_options: HashMap<String, String> = HashMap::new();
        cloud_options.insert(
            "azure_storage_access_key".to_string(),
            "${AZURE_STORAGE_KEY}".to_string(),
        );
        let src = LocalStoreSource::new(
            path.to_string(),
            key_column.to_string(),
            fields.clone(),
            format,
            local_path,
            cloud_options,
        )
        .unwrap();
        let r = src
            .lookup(
                &Value::Int(1),
                &["imdbId".to_string(), "tmdbId".to_string()],
            )
            .await;
        assert_eq!(r[0], Value::Int(114709));
        assert_eq!(r[1], Value::Int(862));
        let r = src
            .lookup(
                &Value::Int(6),
                &["imdbId".to_string(), "tmdbId".to_string()],
            )
            .await;
        assert_eq!(r[0], Value::Int(113277));
        assert_eq!(r[1], Value::Int(949));
    }
}
