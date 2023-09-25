use serde::{Deserialize, Serialize};

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

/// Field
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
struct Field {
    name: String,
    data_type: DataType,
}

/// Schema
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
struct Schema {
    fields: Vec<Field>,
}

/// Table
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
struct Table {
    name: String,
    path: Vec<String>,
    schema: Schema,
    size: i64,
}

/// Dataset
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
struct Dataset {
    tables: Vec<Table>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
struct Query {
    dataset: Dataset,
    query: String,
}


#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn test_query_serialize() {
        let query = Query {
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
        };

        println!("{}", serde_json::to_string(&query).unwrap());
    }

    #[test]
    fn test_query_deserialize() {
        let query_str = r#"{"dataset":{"tables":[{"name":"table_1","path":["schema","table_1"],"schema":{"fields":[{"name":"a","data_type":"Float"},{"name":"b","data_type":"Integer"}]},"size":10000}]},"query":"SELECT * FROM table_1"}"#;
        let query: Query = serde_json::from_str(&query_str).unwrap();
        println!("{:?}", query);
    }
}