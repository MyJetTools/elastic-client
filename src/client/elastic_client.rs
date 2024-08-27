use std::str::FromStr;
use chrono::{DateTime, Utc};

use elasticsearch::{
    http::{
        headers::{HeaderName, HeaderValue},
        response::Response,
        transport::Transport,
    },
    indices::IndicesCreateParts,
    Elasticsearch, Error, IndexParts,
};

pub enum ElasticClientAuth {
    SingleNode {
        url: String,
        esecure: Option<String>,
    },
}

pub enum ElasticIndexRotationPattern {
    Day,
    Mouth,
    Year,
    Hour,
    Minute,
}

pub struct ElasticClient {
    pub elastic_client: Elasticsearch,
    pub esecure: Option<String>,
}

impl ElasticClient {
    pub fn new(auth: ElasticClientAuth) -> Result<Self, elasticsearch::Error> {
        match auth {
            ElasticClientAuth::SingleNode { url, esecure } => {
                let transport = Transport::single_node(&url)?;
                let eclient = Elasticsearch::new(transport);

                Ok(Self {
                    elastic_client: eclient,
                    esecure,
                })
            }
        }
    }

    pub fn get_index_name_with_pattern(
        &self,
        index_name: &str,
        pattern: ElasticIndexRotationPattern,
    ) -> String {
        let date_index = get_time_index(Utc::now(), pattern);
        format!("{}-{}", index_name, date_index)
    }

    pub async fn create_index_mapping(
        &self,
        index_name: &str,
        pattern: ElasticIndexRotationPattern,
        mapping: serde_json::Value,
    ) -> Result<Response, Error> {
        let index_name = self.get_index_name_with_pattern(index_name, pattern);

        self.elastic_client
            .indices()
            .create(IndicesCreateParts::Index(&index_name))
            .body(mapping)
            .header(
                HeaderName::from_str("esecure").unwrap(),
                HeaderValue::from_str(self.esecure.as_ref().unwrap_or(&String::from("default")))
                    .unwrap(),
            )
            .send()
            .await
    }

    pub async fn write_entity(
        &self,
        index_name: &str,
        pattern: ElasticIndexRotationPattern,
        entity: serde_json::Value,
    ) -> Result<Response, Error> {
        let index_name = self.get_index_name_with_pattern(index_name, pattern);

        self.elastic_client
            .index(IndexParts::Index(&index_name))
            .body(entity)
            .header(
                HeaderName::from_str("esecure").unwrap(),
                HeaderValue::from_str(self.esecure.as_ref().unwrap_or(&String::from("default")))
                    .unwrap(),
            )
            .send()
            .await
    }
}

fn get_time_index(current_date: DateTime<Utc>, pattern: ElasticIndexRotationPattern) -> i32 {
    let fmt = match pattern {
        ElasticIndexRotationPattern::Day => "%Y%m%d",
        ElasticIndexRotationPattern::Mouth => "%Y%m",
        ElasticIndexRotationPattern::Year => "%Y",
        ElasticIndexRotationPattern::Hour => "%Y%m%d%H",
        ElasticIndexRotationPattern::Minute => "%Y%m%d%H%M",
    };

    let date = current_date.format(fmt).to_string();
    date.parse::<i32>().unwrap()
}