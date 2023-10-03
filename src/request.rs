use std::{sync::Arc, ops::Deref};
use serde::{Deserialize, Serialize};
use qrlew::{self, Ready as _, Relation, With as _, ast::Query};
use super::*;

/// Simplified DataType
#[derive(Copy, Clone, Debug, PartialEq, PartialOrd, Deserialize, Serialize)]
enum DataType {
    Boolean,
    Integer,
    Float,
    Text,
    Bytes,
    Date,
    Time,
    DateTime,
    Duration,
    Id,
}

impl From<DataType> for qrlew::DataType {
    fn from(value: DataType) -> Self {
        match value {
            DataType::Boolean => qrlew::DataType::boolean(),
            DataType::Integer => qrlew::DataType::integer(),
            DataType::Float => qrlew::DataType::float(),
            DataType::Text => qrlew::DataType::text(),
            DataType::Bytes => qrlew::DataType::bytes(),
            DataType::Date => qrlew::DataType::date(),
            DataType::Time => qrlew::DataType::time(),
            DataType::DateTime => qrlew::DataType::date_time(),
            DataType::Duration => qrlew::DataType::duration(),
            DataType::Id => qrlew::DataType::id(),
        }
    }
}

/// Field
#[derive(Clone, Debug, PartialEq, PartialOrd, Deserialize, Serialize)]
struct Field {
    name: String,
    data_type: DataType,
}

impl From<Field> for qrlew::relation::Field {
    fn from(value: Field) -> Self {
        qrlew::relation::Field::from_name_data_type(value.name, value.data_type)
    }
}

/// Schema
#[derive(Clone, Debug, PartialEq, PartialOrd, Deserialize, Serialize)]
struct Schema {
    fields: Vec<Field>,
}

impl From<Schema> for qrlew::relation::Schema {
    fn from(value: Schema) -> Self {
        qrlew::relation::Schema::from_iter(value.fields.into_iter().map(|f| qrlew::relation::Field::from(f)))
    }
}

/// Table
#[derive(Clone, Debug, PartialEq, PartialOrd, Deserialize, Serialize)]
struct Table {
    name: String,
    path: Vec<String>,
    schema: Schema,
    size: i64,
}

impl From<Table> for qrlew::Relation {
    fn from(value: Table) -> Self {
        qrlew::Relation::table()
            .name(value.name)
            .path(value.path)
            .schema(value.schema)
            .size(value.size)
            .build()
    }
}

/// Dataset
#[derive(Clone, Debug, PartialEq, PartialOrd, Deserialize, Serialize)]
struct Dataset {
    tables: Vec<Table>,
}

impl From<Dataset> for qrlew::hierarchy::Hierarchy<Arc<qrlew::Relation>> {
    fn from(value: Dataset) -> Self {
        value.tables.into_iter().map(|t| (t.path.clone(), Arc::new(qrlew::Relation::from(t)))).collect()
    }
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct Dot {
    dataset: Dataset,
    query: String,
    dark_mode: bool,
}

impl Dot {
    pub fn response(self) -> Result<Response> {
        let query = qrlew::sql::relation::parse(&self.query)?;
        let mut response = Vec::new();
        Relation::try_from(query.with(&self.dataset.into()))?.dot(&mut response, if self.dark_mode {&["dark"]} else {&[]})?;
        Ok(Response::new(String::from_utf8(response)?))
    }
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct Protect {
    dataset: Dataset,
    query: String,
    protected_entity: Vec<(String, Vec<(String, String, String)>, String)>,
}

impl Protect {
    pub fn response(self) -> Result<Response> {
        let query = qrlew::sql::relation::parse(&self.query)?;
        let relations = self.dataset.into();
        let relation = Relation::try_from(query.with(&relations)).unwrap();
        let protected_entity = self.protected_entity.clone();
        let borrowed_protected_entity = protected_entity.iter().map(|(source, links, protected_col)| (source.as_str(), links.iter().map(|(source_col, target, target_col)| (source_col.as_str(), target.as_str(), target_col.as_str())).collect(), protected_col.as_str())).collect();
        let protected_relation = relation.force_protect_from_field_paths(&relations, borrowed_protected_entity);
        Ok(Response::new(Query::from(protected_relation.deref()).to_string()))
    }
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct DPCompile {
    dataset: Dataset,
    query: String,
    protected_entity: Vec<(String, Vec<(String, String, String)>, String)>,
    epsilon: f64,
    delta: f64,
    epsilon_tau_thresholding: f64,
    delta_tau_thresholding: f64,
}

impl DPCompile {
    pub fn response(self, auth: &Authenticator) -> Result<Response> {
        let query = qrlew::sql::relation::parse(&self.query)?;
        let relations = self.dataset.into();
        let relation = Relation::try_from(query.with(&relations)).unwrap();
        let protected_entity = self.protected_entity.clone();
        let borrowed_protected_entity = protected_entity.iter().map(|(source, links, protected_col)| (source.as_str(), links.iter().map(|(source_col, target, target_col)| (source_col.as_str(), target.as_str(), target_col.as_str())).collect(), protected_col.as_str())).collect();
        let protected_relation = relation.force_protect_from_field_paths(&relations, borrowed_protected_entity);
        let dp_relation = protected_relation.dp_compile(self.epsilon, self.delta, self.epsilon_tau_thresholding, self.delta_tau_thresholding)?;
        Ok(Response::signed(Query::from(dp_relation.deref()).to_string(), auth))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn test_dot_serialize() {
        let request = Dot {
            dataset: Dataset { tables: vec![
                Table {
                    name: "table_1".to_string(),
                    path: vec!["schema".to_string(), "table_1".to_string()],
                    schema: Schema { fields: vec![
                        Field { name: "a".to_string(), data_type: DataType::Float },
                        Field { name: "b".to_string(), data_type: DataType::Integer },
                    ]},
                    size: 10000 }
            ]},
            query: "SELECT * FROM table_1".to_string(),
            dark_mode: true,
        };

        println!("{}", serde_json::to_string_pretty(&request).unwrap());
        println!("{}", serde_json::to_string(&request).unwrap());
    }

    #[test]
    fn test_dot_deserialize() {
        let request_str = r#"{"dataset":{"tables":[{"name":"table_1","path":["schema","table_1"],"schema":{"fields":[{"name":"a","data_type":"Float"},{"name":"b","data_type":"Integer"}]},"size":10000}]},"query":"SELECT * FROM table_1","dark_mode":true}"#;
        let request: Dot = serde_json::from_str(&request_str).unwrap();
        println!("{:?}", request);
    }

    #[test]
    fn test_dot() {
        let request_str = r#"{"dataset":{"tables":[{"name":"table_1","path":["schema","table_1"],"schema":{"fields":[{"name":"a","data_type":"Float"},{"name":"b","data_type":"Integer"}]},"size":10000}]},"query":"SELECT * FROM table_1","dark_mode":false}"#;
        let request: Dot = serde_json::from_str(&request_str).unwrap();
        println!("{:?}", request.response().unwrap());
    }

    #[test]
    fn test_protect_serialize() {
        let request = Protect {
            dataset: Dataset { tables: vec![
                Table {
                    name: "user_table".to_string(),
                    path: vec!["schema".to_string(), "user_table".to_string()],
                    schema: Schema { fields: vec![
                        Field { name: "id".to_string(), data_type: DataType::Integer },
                        Field { name: "name".to_string(), data_type: DataType::Text },
                        Field { name: "age".to_string(), data_type: DataType::Integer },
                        Field { name: "weight".to_string(), data_type: DataType::Float },
                    ]},
                    size: 10000,
                },
                Table {
                    name: "action_table".to_string(),
                    path: vec!["schema".to_string(), "action_table".to_string()],
                    schema: Schema { fields: vec![
                        Field { name: "action".to_string(), data_type: DataType::Text },
                        Field { name: "user_id".to_string(), data_type: DataType::Integer },
                        Field { name: "duration".to_string(), data_type: DataType::Float },
                    ]},
                    size: 10000,
                },
            ]},
            query: "SELECT * FROM action_table".to_string(),
            protected_entity: vec![
                ("user_table".to_string(), vec![], "id".to_string()),
                ("action_table".to_string(), vec![("user_id".to_string(), "user_table".to_string(), "id".to_string())], "id".to_string()),
            ],
        };

        println!("{}", serde_json::to_string_pretty(&request).unwrap());
        println!("{}", serde_json::to_string(&request).unwrap());
    }

    #[test]
    fn test_protect_deserialize() {
        let request_str = r#"{"dataset":{"tables":[{"name":"user_table","path":["schema","user_table"],"schema":{"fields":[{"name":"id","data_type":"Integer"},{"name":"name","data_type":"Text"},{"name":"age","data_type":"Integer"},{"name":"weight","data_type":"Float"}]},"size":10000},{"name":"action_table","path":["schema","action_table"],"schema":{"fields":[{"name":"action","data_type":"Text"},{"name":"user_id","data_type":"Integer"},{"name":"duration","data_type":"Float"}]},"size":10000}]},"query":"SELECT * FROM action_table","protected_entity":[["user_table",[],"id"],["action_table",[["user_id","user_table","id"]],"id"]]}"#;
        let request: Protect = serde_json::from_str(&request_str).unwrap();
        println!("{:?}", request);
    }

    #[test]
    fn test_protect() {
        let request_str = r#"{"dataset":{"tables":[{"name":"user_table","path":["schema","user_table"],"schema":{"fields":[{"name":"id","data_type":"Integer"},{"name":"name","data_type":"Text"},{"name":"age","data_type":"Integer"},{"name":"weight","data_type":"Float"}]},"size":10000},{"name":"action_table","path":["schema","action_table"],"schema":{"fields":[{"name":"action","data_type":"Text"},{"name":"user_id","data_type":"Integer"},{"name":"duration","data_type":"Float"}]},"size":10000}]},"query":"SELECT * FROM action_table","protected_entity":[["user_table",[],"id"],["action_table",[["user_id","user_table","id"]],"id"]]}"#;
        let request: Protect = serde_json::from_str(&request_str).unwrap();
        println!("{:?}", request.response().unwrap());
    }

    #[test]
    fn test_dp_compile_serialize() {
        let request = DPCompile {
            dataset: Dataset { tables: vec![
                Table {
                    name: "user_table".to_string(),
                    path: vec!["schema".to_string(), "user_table".to_string()],
                    schema: Schema { fields: vec![
                        Field { name: "id".to_string(), data_type: DataType::Integer },
                        Field { name: "name".to_string(), data_type: DataType::Text },
                        Field { name: "age".to_string(), data_type: DataType::Integer },
                        Field { name: "weight".to_string(), data_type: DataType::Float },
                    ]},
                    size: 10000,
                },
                Table {
                    name: "action_table".to_string(),
                    path: vec!["schema".to_string(), "action_table".to_string()],
                    schema: Schema { fields: vec![
                        Field { name: "action".to_string(), data_type: DataType::Text },
                        Field { name: "user_id".to_string(), data_type: DataType::Integer },
                        Field { name: "duration".to_string(), data_type: DataType::Float },
                    ]},
                    size: 10000,
                },
            ]},
            query: "SELECT sum(duration) FROM action_table WHERE duration > 0 AND duration < 24".to_string(),
            protected_entity: vec![
                ("user_table".to_string(), vec![], "id".to_string()),
                ("action_table".to_string(), vec![("user_id".to_string(), "user_table".to_string(), "id".to_string())], "id".to_string()),
            ],
            epsilon: 1.,
            delta: 1e-5,
            epsilon_tau_thresholding: 1.,
            delta_tau_thresholding: 1e-5,
        };

        println!("{}", serde_json::to_string_pretty(&request).unwrap());
        println!("{}", serde_json::to_string(&request).unwrap());
    }

    #[test]
    fn test_dp_compile_deserialize() {
        let request_str = r#"
{"dataset":{"tables":[{"name":"user_table","path":["schema","user_table"],"schema":{"fields":[{"name":"id","data_type":"Integer"},{"name":"name","data_type":"Text"},{"name":"age","data_type":"Integer"},{"name":"weight","data_type":"Float"}]},"size":10000},{"name":"action_table","path":["schema","action_table"],"schema":{"fields":[{"name":"action","data_type":"Text"},{"name":"user_id","data_type":"Integer"},{"name":"duration","data_type":"Float"}]},"size":10000}]},"query":"SELECT sum(duration) FROM action_table WHERE duration > 0 AND duration < 24","protected_entity":[["user_table",[],"id"],["action_table",[["user_id","user_table","id"]],"id"]],"epsilon":1.0,"delta":0.00001,"epsilon_tau_thresholding":1.0,"delta_tau_thresholding":0.00001}
"#;
        let request: DPCompile = serde_json::from_str(&request_str).unwrap();
        println!("{:?}", request);
    }

    #[test]
    fn test_dp_compile() {
        let auth = Authenticator::random_2048().unwrap();
        let request_str = r#"
{"dataset":{"tables":[{"name":"user_table","path":["schema","user_table"],"schema":{"fields":[{"name":"id","data_type":"Integer"},{"name":"name","data_type":"Text"},{"name":"age","data_type":"Integer"},{"name":"weight","data_type":"Float"}]},"size":10000},{"name":"action_table","path":["schema","action_table"],"schema":{"fields":[{"name":"action","data_type":"Text"},{"name":"user_id","data_type":"Integer"},{"name":"duration","data_type":"Float"}]},"size":10000}]},"query":"SELECT sum(duration) FROM action_table WHERE duration > 0 AND duration < 24","protected_entity":[["user_table",[],"id"],["action_table",[["user_id","user_table","id"]],"id"]],"epsilon":1.0,"delta":0.00001,"epsilon_tau_thresholding":1.0,"delta_tau_thresholding":0.00001}
"#;
        let request: DPCompile = serde_json::from_str(&request_str).unwrap();
        println!("{:?}", request.response(&auth).unwrap());
    }
}