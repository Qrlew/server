use std::{sync::Arc, convert::TryFrom};
use serde::{Deserialize, Serialize, Deserializer};
use serde_json::Value;
use chrono::{NaiveDate, NaiveTime, NaiveDateTime, Duration};
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

/// Convert Field into qrlew DataType
fn data_type_from_field(value: Field) -> Option<qrlew::DataType> {
    Some(match value {
        Field {
            name: _,
            data_type,
            range: None,
            possible_values: None,
            constraint: _,
        } => match data_type {
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
        },
        Field {
            name,
            data_type,
            range: Some((min, max)),
            possible_values: None,
            constraint: _,
        } => match data_type {
            DataType::Boolean => qrlew::DataType::boolean_interval(min.as_bool()?, max.as_bool()?),
            DataType::Integer => qrlew::DataType::integer_interval(min.as_i64()?, max.as_i64()?),
            DataType::Float => qrlew::DataType::float_interval(min.as_f64()?, max.as_f64()?),
            DataType::Text => qrlew::DataType::text_interval(min.as_str()?.to_string(), max.as_str()?.to_string()),
            DataType::Date => qrlew::DataType::date_interval(NaiveDate::parse_from_str(min.as_str()?, "%Y-%m-%d").ok()?, NaiveDate::parse_from_str(max.as_str()?, "%Y-%m-%d").ok()?),
            DataType::Time => qrlew::DataType::time_interval(NaiveTime::parse_from_str(min.as_str()?, "%H:%M:%S").ok()?, NaiveTime::parse_from_str(max.as_str()?, "%H:%M:%S").ok()?),
            DataType::DateTime => qrlew::DataType::date_time_interval(NaiveDateTime::parse_from_str(min.as_str()?, "%Y-%m-%d %H:%M:%S").ok()?, NaiveDateTime::parse_from_str(max.as_str()?, "%Y-%m-%d %H:%M:%S").ok()?),
            DataType::Duration => qrlew::DataType::duration_interval(Duration::seconds(min.as_i64()?), Duration::seconds(max.as_i64()?)),
            DataType::Id => qrlew::DataType::id(),
            _ => None?,
        },
        Field {
            name: _,
            data_type,
            range: None,
            possible_values: Some(possible_values),
            constraint: _,
        } => match data_type {
            DataType::Boolean => qrlew::DataType::boolean_values(possible_values.into_iter().filter_map(|v| v.as_bool()).collect::<Vec<_>>()),
            DataType::Integer => qrlew::DataType::integer_values(possible_values.into_iter().filter_map(|v| v.as_i64()).collect::<Vec<_>>()),
            DataType::Float => qrlew::DataType::float_values(possible_values.into_iter().filter_map(|v| v.as_f64()).collect::<Vec<_>>()),
            DataType::Text => qrlew::DataType::text_values(possible_values.into_iter().filter_map(|v| Some(v.as_str()?.to_string())).collect::<Vec<_>>()),
            DataType::Date => qrlew::DataType::date_values(possible_values.into_iter().filter_map(|v| NaiveDate::parse_from_str(v.as_str()?, "%Y-%m-%d").ok()).collect::<Vec<_>>()),
            DataType::Time => qrlew::DataType::time_values(possible_values.into_iter().filter_map(|v| NaiveTime::parse_from_str(v.as_str()?, "%H:%M:%S").ok()).collect::<Vec<_>>()),
            DataType::DateTime => qrlew::DataType::date_time_values(possible_values.into_iter().filter_map(|v| NaiveDateTime::parse_from_str(v.as_str()?, "%Y-%m-%d %H:%M:%S").ok()).collect::<Vec<_>>()),
            DataType::Duration => qrlew::DataType::duration_values(possible_values.into_iter().filter_map(|v| Some(Duration::seconds(v.as_i64()?))).collect::<Vec<_>>()),
            DataType::Id => qrlew::DataType::id(),
            _ => None?,
        },
        field => None?,
    })
}

impl TryFrom<Field> for qrlew::DataType {
    type Error = Error;

    fn try_from(value: Field) -> Result<Self> {
        let err = Error::other(value.name.clone());
        data_type_from_field(value).ok_or(err)
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
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
struct Field {
    name: String,
    data_type: DataType,
    range: Option<(Value, Value)>,
    possible_values: Option<Vec<Value>>,
    constraint: Option<Constraint>,
}

impl From<Field> for qrlew::relation::Field {
    fn from(value: Field) -> Self {
        let data_type = value.clone().try_into().unwrap();
        qrlew::relation::Field::new(value.name, data_type, value.constraint.map(Constraint::into))
    }
}

/// Schema
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
struct Schema {
    fields: Vec<Field>,
}

impl From<Schema> for qrlew::relation::Schema {
    fn from(value: Schema) -> Self {
        qrlew::relation::Schema::from_iter(value.fields.into_iter().map(|f| qrlew::relation::Field::from(f)))
    }
}

/// Table
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
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
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
struct Dataset {
    tables: Vec<Table>,
}

impl From<Dataset> for qrlew::hierarchy::Hierarchy<Arc<qrlew::Relation>> {
    fn from(value: Dataset) -> Self {
        value.tables.into_iter().map(|t| (t.path.clone(), Arc::new(qrlew::Relation::from(t)))).collect()
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
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

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
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
        let pup_relation = relation.rewrite_as_privacy_unit_preserving(&relations, synthetic_data, privacy_unit, budget)?;
        Ok(Response::new(Query::from(pup_relation.relation()).to_string()))
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct RewriteWithDifferentialPrivacy {
    dataset: Dataset,
    query: String,
    synthetic_data: Vec<(String, String)>,
    privacy_unit: Vec<(String, Vec<(String, String, String)>, String)>,
    epsilon: f64,
    delta: f64,
}

impl RewriteWithDifferentialPrivacy {
    pub fn response(self, auth: &Authenticator) -> Result<Response> {
        let query = qrlew::sql::relation::parse(&self.query)?;
        let relations = self.dataset.into();
        let relation = Relation::try_from(query.with(&relations)).unwrap();
        let synthetic_data = SyntheticData::new(self.synthetic_data.into_iter().map(|(table, synthetic_table)| (Identifier::from(table), Identifier::from(synthetic_table))).collect());
        let borrowed_privacy_unit: Vec<(&str, Vec<(&str, &str, &str)>, &str)> = self.privacy_unit.iter().map(|(source, links, privacy_unit)| (source.as_str(), links.iter().map(|(source_col, target, target_col)| (source_col.as_str(), target.as_str(), target_col.as_str())).collect(), privacy_unit.as_str())).collect();
        let privacy_unit = PrivacyUnit::from(borrowed_privacy_unit);
        let budget = Budget::new(self.epsilon, self.delta);
        let dp_relation = relation.rewrite_with_differential_privacy(&relations, synthetic_data, privacy_unit, budget)?;
        Ok(Response::signed(Query::from(dp_relation.relation()).to_string(), auth))
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
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

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct RewriteAsPrivacyUnitPreservingWithDot {
    dataset: Dataset,
    query: String,
    synthetic_data: Vec<(String, String)>,
    privacy_unit: Vec<(String, Vec<(String, String, String)>, String)>,
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
        let borrowed_privacy_unit: Vec<(&str, Vec<(&str, &str, &str)>, &str)> = self.privacy_unit.iter().map(|(source, links, privacy_unit)| (source.as_str(), links.iter().map(|(source_col, target, target_col)| (source_col.as_str(), target.as_str(), target_col.as_str())).collect(), privacy_unit.as_str())).collect();
        let privacy_unit = PrivacyUnit::from(borrowed_privacy_unit);
        let budget = Budget::new(self.epsilon, self.delta);
        let pup_relation = relation.rewrite_as_privacy_unit_preserving(&relations, synthetic_data, privacy_unit, budget)?;
        let mut dot = Vec::new();
        pup_relation.relation().dot(&mut dot, if self.dark_mode {&["dark"]} else {&[]})?;
        Ok(Response::new(serde_json::to_string(&QueryWithDot::new(Query::from(pup_relation.relation()).to_string(), String::from_utf8(dot)?))?))
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct RewriteWithDifferentialPrivacyWithDot {
    dataset: Dataset,
    query: String,
    synthetic_data: Vec<(String, String)>,
    privacy_unit: Vec<(String, Vec<(String, String, String)>, String)>,
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
        let borrowed_privacy_unit: Vec<(&str, Vec<(&str, &str, &str)>, &str)> = self.privacy_unit.iter().map(|(source, links, privacy_unit)| (source.as_str(), links.iter().map(|(source_col, target, target_col)| (source_col.as_str(), target.as_str(), target_col.as_str())).collect(), privacy_unit.as_str())).collect();
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
                        Field { name: "a".to_string(), data_type: DataType::Float, constraint: None, range: None, possible_values: None },
                        Field { name: "b".to_string(), data_type: DataType::Integer, constraint: Some(Constraint::Unique), range: None, possible_values: None },
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
    fn test_rewrite_as_pup_serialize() {
        let request = RewriteAsPrivacyUnitPreserving {
            dataset: Dataset { tables: vec![
                Table {
                    name: "user_table".to_string(),
                    path: vec!["schema".to_string(), "user_table".to_string()],
                    schema: Schema { fields: vec![
                        Field { name: "id".to_string(), data_type: DataType::Integer, constraint: Some(Constraint::Unique), range: None, possible_values: None },
                        Field { name: "name".to_string(), data_type: DataType::Text, constraint: None, range: None, possible_values: None },
                        Field { name: "age".to_string(), data_type: DataType::Integer, constraint: None, range: None, possible_values: None },
                        Field { name: "weight".to_string(), data_type: DataType::Float, constraint: None, range: None, possible_values: None },
                    ]},
                    size: 10000,
                },
                Table {
                    name: "action_table".to_string(),
                    path: vec!["schema".to_string(), "action_table".to_string()],
                    schema: Schema { fields: vec![
                        Field { name: "action".to_string(), data_type: DataType::Text, constraint: None, range: None, possible_values: None },
                        Field { name: "user_id".to_string(), data_type: DataType::Integer, constraint: None, range: None, possible_values: None },
                        Field { name: "duration".to_string(), data_type: DataType::Float, constraint: None, range: None, possible_values: None },
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
    fn test_rewrite_as_pup_deserialize() {
        let request_str = r#"{"dataset":{"tables":[{"name":"user_table","path":["schema","user_table"],"schema":{"fields":[{"name":"id","data_type":"Integer"},{"name":"name","data_type":"Text"},{"name":"age","data_type":"Integer"},{"name":"weight","data_type":"Float"}]},"size":10000},{"name":"action_table","path":["schema","action_table"],"schema":{"fields":[{"name":"action","data_type":"Text"},{"name":"user_id","data_type":"Integer"},{"name":"duration","data_type":"Float"}]},"size":10000}]},"query":"SELECT * FROM action_table","synthetic_data":[["user_table","synthetic_user_table"],["action_table","synthetic_action_table"]],"privacy_unit":[["user_table",[],"id"],["action_table",[["user_id","user_table","id"]],"id"]],"epsilon":1.0,"delta":0.00001}"#;
        let request: RewriteAsPrivacyUnitPreserving = serde_json::from_str(&request_str).unwrap();
        println!("{:?}", request);
    }

    #[test]
    fn test_rewrite_as_pup() {
        let request_str = r#"{"dataset":{"tables":[{"name":"user_table","path":["schema","user_table"],"schema":{"fields":[{"name":"id","data_type":"Integer"},{"name":"name","data_type":"Text"},{"name":"age","data_type":"Integer"},{"name":"weight","data_type":"Float"}]},"size":10000},{"name":"action_table","path":["schema","action_table"],"schema":{"fields":[{"name":"action","data_type":"Text"},{"name":"user_id","data_type":"Integer"},{"name":"duration","data_type":"Float"}]},"size":10000}]},"query":"SELECT * FROM action_table","synthetic_data":[["user_table","synthetic_user_table"],["action_table","synthetic_action_table"]],"privacy_unit":[["user_table",[],"id"],["action_table",[["user_id","user_table","id"]],"id"]],"epsilon":1.0,"delta":0.00001}"#;
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
                        Field { name: "id".to_string(), data_type: DataType::Integer, constraint: Some(Constraint::Unique), range: None, possible_values: None },
                        Field { name: "name".to_string(), data_type: DataType::Text, constraint: None, range: None, possible_values: None },
                        Field { name: "age".to_string(), data_type: DataType::Integer, constraint: None, range: None, possible_values: None },
                        Field { name: "weight".to_string(), data_type: DataType::Float, constraint: None, range: None, possible_values: None },
                    ]},
                    size: 10000,
                },
                Table {
                    name: "action_table".to_string(),
                    path: vec!["schema".to_string(), "action_table".to_string()],
                    schema: Schema { fields: vec![
                        Field { name: "action".to_string(), data_type: DataType::Text, constraint: None, range: None, possible_values: None },
                        Field { name: "user_id".to_string(), data_type: DataType::Integer, constraint: None, range: None, possible_values: None },
                        Field { name: "duration".to_string(), data_type: DataType::Float, constraint: None, range: None, possible_values: None },
                    ]},
                    size: 10000,
                },
            ]},
            query: "SELECT sum(duration) FROM action_table WHERE duration > 0 AND duration < 24".to_string(),
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
    fn test_rewrite_with_dp_deserialize() {
        let request_str = r#"
        {"dataset":{"tables":[{"name":"user_table","path":["schema","user_table"],"schema":{"fields":[{"name":"id","data_type":"Integer"},{"name":"name","data_type":"Text"},{"name":"age","data_type":"Integer"},{"name":"weight","data_type":"Float"}]},"size":10000},{"name":"action_table","path":["schema","action_table"],"schema":{"fields":[{"name":"action","data_type":"Text"},{"name":"user_id","data_type":"Integer"},{"name":"duration","data_type":"Float"}]},"size":10000}]},"query":"SELECT sum(duration) FROM action_table WHERE duration > 0 AND duration < 24","synthetic_data":[["user_table","synthetic_user_table"],["action_table","synthetic_action_table"]],"privacy_unit":[["user_table",[],"id"],["action_table",[["user_id","user_table","id"]],"id"]],"epsilon":1.0,"delta":0.00001}
"#;
        let request: RewriteWithDifferentialPrivacy = serde_json::from_str(&request_str).unwrap();
        println!("{:?}", request);
    }

    #[test]
    fn test_rewrite_with_dp() {
        let auth = Authenticator::get("secret_key.pem").unwrap();
        let request_str = r#"
        {"dataset":{"tables":[{"name":"user_table","path":["schema","user_table"],"schema":{"fields":[{"name":"id","data_type":"Integer"},{"name":"name","data_type":"Text"},{"name":"age","data_type":"Integer"},{"name":"weight","data_type":"Float"}]},"size":10000},{"name":"action_table","path":["schema","action_table"],"schema":{"fields":[{"name":"action","data_type":"Text"},{"name":"user_id","data_type":"Integer"},{"name":"duration","data_type":"Float"}]},"size":10000}]},"query":"SELECT sum(duration) FROM action_table WHERE duration > 0 AND duration < 24","synthetic_data":[["user_table","synthetic_user_table"],["action_table","synthetic_action_table"]],"privacy_unit":[["user_table",[],"id"],["action_table",[["user_id","user_table","id"]],"id"]],"epsilon":1.0,"delta":0.00001}
"#;
        let request: RewriteWithDifferentialPrivacy = serde_json::from_str(&request_str).unwrap();
        println!("{:?}", request.response(&auth).unwrap());
    }
}