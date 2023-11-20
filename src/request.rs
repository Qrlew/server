use std::{sync::Arc, ops::Deref};
use serde::{Deserialize, Serialize};
use serde_json;
use qrlew::{self, Ready as _, Relation, With as _, ast::{Query, self}, expr::Identifier, synthetic_data::SyntheticData,
privacy_unit_tracking::PrivacyUnit, differential_privacy::budget::Budget};
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

/// Simplified Constraint
#[derive(Copy, Clone, Debug, PartialEq, PartialOrd, Deserialize, Serialize)]
enum Constraint {
    Unique,
}

impl From<Constraint> for qrlew::relation::Constraint {
    fn from(value: Constraint) -> Self {
        match value {
            Constraint::Unique => qrlew::relation::Constraint::Unique,
        }
    }
}

/// Field
#[derive(Clone, Debug, PartialEq, PartialOrd, Deserialize, Serialize)]
struct Field {
    name: String,
    data_type: DataType,
    constraint: Option<Constraint>,
}

impl From<Field> for qrlew::relation::Field {
    fn from(value: Field) -> Self {
        qrlew::relation::Field::new(value.name, value.data_type.into(), value.constraint.map(Constraint::into))
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
pub struct RewriteAsPrivacyUnitPreserving {
    dataset: Dataset,
    query: String,
    synthetic_data: Vec<(String, String)>,
    privacy_unit: Vec<(String, Vec<(String, String, String)>, String)>,
    epsilon: f64,
    delta: f64,
}

impl RewriteAsPrivacyUnitPreserving {
    pub fn response(self) -> Result<Response> {
        let query = qrlew::sql::relation::parse(&self.query)?;
        let relations = self.dataset.into();
        let relation = Relation::try_from(query.with(&relations)).unwrap();
        let synthetic_data = SyntheticData::new(self.synthetic_data.into_iter().map(|(table, synthetic_table)| (Identifier::from(table), Identifier::from(synthetic_table))).collect());
        let borrowed_privacy_unit: Vec<(&str, Vec<(&str, &str, &str)>, &str)> = self.privacy_unit.iter().map(|(source, links, privacy_unit)| (source.as_str(), links.iter().map(|(source_col, target, target_col)| (source_col.as_str(), target.as_str(), target_col.as_str())).collect(), privacy_unit.as_str())).collect();
        let privacy_unit = PrivacyUnit::from(borrowed_privacy_unit);
        let budget = Budget::new(self.epsilon, self.delta);
        let pep_relation = relation.rewrite_as_privacy_unit_preserving(&relations, synthetic_data, privacy_unit, budget)?;
        Ok(Response::new(Query::from(pep_relation.relation()).to_string()))
    }
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct RewriteWithDifferentialPrivacy {
    dataset: Dataset,
    query: String,
    synthetic_data: Vec<(String, String)>,
    protected_entity: Vec<(String, Vec<(String, String, String)>, String)>,
    epsilon: f64,
    delta: f64,
}

impl RewriteWithDifferentialPrivacy {
    pub fn response(self, auth: &Authenticator) -> Result<Response> {
        let query = qrlew::sql::relation::parse(&self.query)?;
        let relations = self.dataset.into();
        let relation = Relation::try_from(query.with(&relations)).unwrap();
        let synthetic_data = SyntheticData::new(self.synthetic_data.into_iter().map(|(table, synthetic_table)| (Identifier::from(table), Identifier::from(synthetic_table))).collect());
        let borrowed_protected_entity: Vec<(&str, Vec<(&str, &str, &str)>, &str)> = self.protected_entity.iter().map(|(source, links, protected_col)| (source.as_str(), links.iter().map(|(source_col, target, target_col)| (source_col.as_str(), target.as_str(), target_col.as_str())).collect(), protected_col.as_str())).collect();
        let protected_entity = PrivacyUnit::from(borrowed_protected_entity);
        let budget = Budget::new(self.epsilon, self.delta);
        let dp_relation = relation.rewrite_with_differential_privacy(&relations, synthetic_data, protected_entity, budget)?;
        Ok(Response::signed(Query::from(dp_relation.relation()).to_string(), auth))
    }
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct QueryWithDot {
    query: String,
    dot: String,
}

impl QueryWithDot {
    pub fn new(query: String, dot: String) -> QueryWithDot {
        QueryWithDot {
            query,
            dot,
        }
    }
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct RewriteAsPrivacyUnitPreservingWithDot {
    dataset: Dataset,
    query: String,
    synthetic_data: Vec<(String, String)>,
    protected_entity: Vec<(String, Vec<(String, String, String)>, String)>,
    epsilon: f64,
    delta: f64,
    dark_mode: bool,
}

impl RewriteAsPrivacyUnitPreservingWithDot {
    pub fn response(self) -> Result<Response> {
        let query = qrlew::sql::relation::parse(&self.query)?;
        let relations = self.dataset.into();
        let relation = Relation::try_from(query.with(&relations)).unwrap();
        let synthetic_data = SyntheticData::new(self.synthetic_data.into_iter().map(|(table, synthetic_table)| (Identifier::from(table), Identifier::from(synthetic_table))).collect());
        let borrowed_privacy_unit: Vec<(&str, Vec<(&str, &str, &str)>, &str)> = self.protected_entity.iter().map(|(source, links, privacy_unit)| (source.as_str(), links.iter().map(|(source_col, target, target_col)| (source_col.as_str(), target.as_str(), target_col.as_str())).collect(), privacy_unit.as_str())).collect();
        let privacy_unit = PrivacyUnit::from(borrowed_privacy_unit);
        let budget = Budget::new(self.epsilon, self.delta);
        let pep_relation = relation.rewrite_as_privacy_unit_preserving(&relations, synthetic_data, privacy_unit, budget)?;
        let mut dot = Vec::new();
        pep_relation.relation().dot(&mut dot, if self.dark_mode {&["dark"]} else {&[]})?;
        Ok(Response::new(serde_json::to_string(&QueryWithDot::new(Query::from(pep_relation.relation()).to_string(), String::from_utf8(dot)?))?))
    }
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct RewriteWithDifferentialPrivacyWithDot {
    dataset: Dataset,
    query: String,
    synthetic_data: Vec<(String, String)>,
    protected_entity: Vec<(String, Vec<(String, String, String)>, String)>,
    epsilon: f64,
    delta: f64,
    dark_mode: bool,
}

impl RewriteWithDifferentialPrivacyWithDot {
    pub fn response(self, auth: &Authenticator) -> Result<Response> {
        let query = qrlew::sql::relation::parse(&self.query)?;
        let relations = self.dataset.into();
        let relation = Relation::try_from(query.with(&relations)).unwrap();
        let synthetic_data = SyntheticData::new(self.synthetic_data.into_iter().map(|(table, synthetic_table)| (Identifier::from(table), Identifier::from(synthetic_table))).collect());
        let borrowed_privacy_unit: Vec<(&str, Vec<(&str, &str, &str)>, &str)> = self.protected_entity.iter().map(|(source, links, protected_col)| (source.as_str(), links.iter().map(|(source_col, target, target_col)| (source_col.as_str(), target.as_str(), target_col.as_str())).collect(), protected_col.as_str())).collect();
        let privacy_unit = PrivacyUnit::from(borrowed_privacy_unit);
        let budget = Budget::new(self.epsilon, self.delta);
        let dp_relation = relation.rewrite_with_differential_privacy(&relations, synthetic_data, privacy_unit, budget)?;
        let mut dot = Vec::new();
        dp_relation.relation().dot(&mut dot, if self.dark_mode {&["dark"]} else {&[]})?;
        Ok(Response::signed(serde_json::to_string(&QueryWithDot::new(Query::from(dp_relation.relation()).to_string(), String::from_utf8(dot)?))?, auth))
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
                        Field { name: "a".to_string(), data_type: DataType::Float, constraint: None },
                        Field { name: "b".to_string(), data_type: DataType::Integer, constraint: Some(Constraint::Unique) },
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
        // println!("{:?}", request.response().unwrap());
        println!("{}", request.response().unwrap().value());
    }

    #[test]
    fn test_rewrite_as_pep_serialize() {
        let request = RewriteAsPrivacyUnitPreserving {
            dataset: Dataset { tables: vec![
                Table {
                    name: "user_table".to_string(),
                    path: vec!["schema".to_string(), "user_table".to_string()],
                    schema: Schema { fields: vec![
                        Field { name: "id".to_string(), data_type: DataType::Integer, constraint: Some(Constraint::Unique) },
                        Field { name: "name".to_string(), data_type: DataType::Text, constraint: None },
                        Field { name: "age".to_string(), data_type: DataType::Integer, constraint: None },
                        Field { name: "weight".to_string(), data_type: DataType::Float, constraint: None },
                    ]},
                    size: 10000,
                },
                Table {
                    name: "action_table".to_string(),
                    path: vec!["schema".to_string(), "action_table".to_string()],
                    schema: Schema { fields: vec![
                        Field { name: "action".to_string(), data_type: DataType::Text, constraint: None },
                        Field { name: "user_id".to_string(), data_type: DataType::Integer, constraint: None },
                        Field { name: "duration".to_string(), data_type: DataType::Float, constraint: None },
                    ]},
                    size: 10000,
                },
            ]},
            query: "SELECT * FROM action_table".to_string(),
            synthetic_data: vec![
                ("user_table".to_string(), "synthetic_user_table".to_string()),
                ("action_table".to_string(), "synthetic_action_table".to_string()),
            ],
            privacy_unit: vec![
                ("user_table".to_string(), vec![], "id".to_string()),
                ("action_table".to_string(), vec![("user_id".to_string(), "user_table".to_string(), "id".to_string())], "id".to_string()),
            ],
            epsilon: 1.,
            delta: 1e-5,
        };

        println!("{}", serde_json::to_string_pretty(&request).unwrap());
        println!("{}", serde_json::to_string(&request).unwrap());
    }

    #[test]
    fn test_rewrite_as_pep_deserialize() {
        let request_str = r#"{"dataset":{"tables":[{"name":"user_table","path":["schema","user_table"],"schema":{"fields":[{"name":"id","data_type":"Integer"},{"name":"name","data_type":"Text"},{"name":"age","data_type":"Integer"},{"name":"weight","data_type":"Float"}]},"size":10000},{"name":"action_table","path":["schema","action_table"],"schema":{"fields":[{"name":"action","data_type":"Text"},{"name":"user_id","data_type":"Integer"},{"name":"duration","data_type":"Float"}]},"size":10000}]},"query":"SELECT * FROM action_table","synthetic_data":[["user_table","synthetic_user_table"],["action_table","synthetic_action_table"]],"protected_entity":[["user_table",[],"id"],["action_table",[["user_id","user_table","id"]],"id"]],"epsilon":1.0,"delta":0.00001}"#;
        let request: RewriteAsPrivacyUnitPreserving = serde_json::from_str(&request_str).unwrap();
        println!("{:?}", request);
    }

    #[test]
    fn test_rewrite_as_pep() {
        let request_str = r#"{"dataset":{"tables":[{"name":"user_table","path":["schema","user_table"],"schema":{"fields":[{"name":"id","data_type":"Integer"},{"name":"name","data_type":"Text"},{"name":"age","data_type":"Integer"},{"name":"weight","data_type":"Float"}]},"size":10000},{"name":"action_table","path":["schema","action_table"],"schema":{"fields":[{"name":"action","data_type":"Text"},{"name":"user_id","data_type":"Integer"},{"name":"duration","data_type":"Float"}]},"size":10000}]},"query":"SELECT * FROM action_table","synthetic_data":[["user_table","synthetic_user_table"],["action_table","synthetic_action_table"]],"protected_entity":[["user_table",[],"id"],["action_table",[["user_id","user_table","id"]],"id"]],"epsilon":1.0,"delta":0.00001}"#;
        let request: RewriteAsPrivacyUnitPreserving = serde_json::from_str(&request_str).unwrap();
        println!("{:?}", request.response().unwrap());
    }

    #[test]
    fn test_rewrite_with_dp_serialize() {
        let request = RewriteWithDifferentialPrivacy {
            dataset: Dataset { tables: vec![
                Table {
                    name: "user_table".to_string(),
                    path: vec!["schema".to_string(), "user_table".to_string()],
                    schema: Schema { fields: vec![
                        Field { name: "id".to_string(), data_type: DataType::Integer, constraint: Some(Constraint::Unique) },
                        Field { name: "name".to_string(), data_type: DataType::Text, constraint: None },
                        Field { name: "age".to_string(), data_type: DataType::Integer, constraint: None },
                        Field { name: "weight".to_string(), data_type: DataType::Float, constraint: None },
                    ]},
                    size: 10000,
                },
                Table {
                    name: "action_table".to_string(),
                    path: vec!["schema".to_string(), "action_table".to_string()],
                    schema: Schema { fields: vec![
                        Field { name: "action".to_string(), data_type: DataType::Text, constraint: None },
                        Field { name: "user_id".to_string(), data_type: DataType::Integer, constraint: None },
                        Field { name: "duration".to_string(), data_type: DataType::Float, constraint: None },
                    ]},
                    size: 10000,
                },
            ]},
            query: "SELECT sum(duration) FROM action_table WHERE duration > 0 AND duration < 24".to_string(),
            synthetic_data: vec![
                ("user_table".to_string(), "synthetic_user_table".to_string()),
                ("action_table".to_string(), "synthetic_action_table".to_string()),
            ],
            protected_entity: vec![
                ("user_table".to_string(), vec![], "id".to_string()),
                ("action_table".to_string(), vec![("user_id".to_string(), "user_table".to_string(), "id".to_string())], "id".to_string()),
            ],
            epsilon: 1.,
            delta: 1e-5,
        };

        println!("{}", serde_json::to_string_pretty(&request).unwrap());
        println!("{}", serde_json::to_string(&request).unwrap());
    }

    #[test]
    fn test_rewrite_with_dp_deserialize() {
        let request_str = r#"
        {"dataset":{"tables":[{"name":"user_table","path":["schema","user_table"],"schema":{"fields":[{"name":"id","data_type":"Integer"},{"name":"name","data_type":"Text"},{"name":"age","data_type":"Integer"},{"name":"weight","data_type":"Float"}]},"size":10000},{"name":"action_table","path":["schema","action_table"],"schema":{"fields":[{"name":"action","data_type":"Text"},{"name":"user_id","data_type":"Integer"},{"name":"duration","data_type":"Float"}]},"size":10000}]},"query":"SELECT sum(duration) FROM action_table WHERE duration > 0 AND duration < 24","synthetic_data":[["user_table","synthetic_user_table"],["action_table","synthetic_action_table"]],"protected_entity":[["user_table",[],"id"],["action_table",[["user_id","user_table","id"]],"id"]],"epsilon":1.0,"delta":0.00001}
"#;
        let request: RewriteWithDifferentialPrivacy = serde_json::from_str(&request_str).unwrap();
        println!("{:?}", request);
    }

    #[test]
    fn test_rewrite_with_dp() {
        let auth = Authenticator::get("secret_key.pem").unwrap();
        let request_str = r#"
        {"dataset":{"tables":[{"name":"user_table","path":["schema","user_table"],"schema":{"fields":[{"name":"id","data_type":"Integer"},{"name":"name","data_type":"Text"},{"name":"age","data_type":"Integer"},{"name":"weight","data_type":"Float"}]},"size":10000},{"name":"action_table","path":["schema","action_table"],"schema":{"fields":[{"name":"action","data_type":"Text"},{"name":"user_id","data_type":"Integer"},{"name":"duration","data_type":"Float"}]},"size":10000}]},"query":"SELECT sum(duration) FROM action_table WHERE duration > 0 AND duration < 24","synthetic_data":[["user_table","synthetic_user_table"],["action_table","synthetic_action_table"]],"protected_entity":[["user_table",[],"id"],["action_table",[["user_id","user_table","id"]],"id"]],"epsilon":1.0,"delta":0.00001}
"#;
        let request: RewriteWithDifferentialPrivacy = serde_json::from_str(&request_str).unwrap();
        println!("{:?}", request.response(&auth).unwrap());
    }
}