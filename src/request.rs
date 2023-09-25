use std::rc::Rc;
use serde::{Deserialize, Serialize};
use qrlew::{self, Ready as _, Relation, With as _};
use super::*;

/// Simplified DataType
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
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
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
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
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
struct Schema {
    fields: Vec<Field>,
}

impl From<Schema> for qrlew::relation::Schema {
    fn from(value: Schema) -> Self {
        qrlew::relation::Schema::from_iter(value.fields.into_iter().map(|f| qrlew::relation::Field::from(f)))
    }
}

/// Table
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
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
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
struct Dataset {
    tables: Vec<Table>,
}

impl From<Dataset> for qrlew::hierarchy::Hierarchy<Rc<qrlew::Relation>> {
    fn from(value: Dataset) -> Self {
        value.tables.into_iter().map(|t| (t.path.clone(), Rc::new(qrlew::Relation::from(t)))).collect()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
pub struct Dot {
    dataset: Dataset,
    query: String,
    dark_mode: bool,
}

// Relation::try_from(parse(query).unwrap().with(&database.relations())).unwrap();
//             relation.display_dot();

impl Dot {
    pub fn response(self) -> Result<String> {
        let query = qrlew::sql::relation::parse(&self.query)?;
        let mut response = Vec::new();
        Relation::try_from(query.with(&self.dataset.into()))?.dot(&mut response, if self.dark_mode {&["dark"]} else {&[]})?;
        Ok(String::from_utf8(response)?)
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
        println!("{}", request.response().unwrap());
    }
}