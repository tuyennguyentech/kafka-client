use kafka_protocol::{
    messages::{
        ApiKey, ApiVersionsRequest, ApiVersionsResponse, RequestHeader, ResponseHeader,
        api_versions_response::ApiVersion,
    },
    protocol::{Decodable as _, HeaderVersion as _, Message as _, StrBytes},
};

use super::KafkaClient;

impl KafkaClient {
    pub async fn request_api_versions(
        &mut self,
        client_software_name: String,
        client_software_version: String,
    ) {
        let header = RequestHeader::default()
            .with_request_api_key(ApiKey::ApiVersions as _)
            .with_request_api_version(ApiVersion::VERSIONS.max)
            .with_client_id(None);
        let body = ApiVersionsRequest::default()
            .with_client_software_name(StrBytes::from_string(client_software_name))
            .with_client_software_version(StrBytes::from_string(client_software_version));
        let res = self.request(header, body).await;
        if let Some(mut buf) = res {
            // println!("size = {}\n{:?}", buf.len(), buf);
            let _header = ResponseHeader::decode(
                &mut buf,
                ApiVersionsResponse::header_version(ApiVersionsResponse::VERSIONS.max),
            )
            .unwrap();
            // println!("{:?}", header);
            let body =
                ApiVersionsResponse::decode(&mut buf, ApiVersionsRequest::VERSIONS.max).unwrap();
            // println!("{:#?}\n{:#?}", header, body);
            self.get_state_mut().api_versions_map.replace(
                body.api_keys
                    .iter()
                    .map(|x| (x.api_key, x.clone()))
                    .collect(),
            );
            println!("{:?}", _header);
            // println!("{:?}\n{:?}", _header, body);
            self.get_state_mut().api_versions.replace(body);
        } else {
            panic!("Cannot fetch api versions of cluster");
        }
    }
    pub fn get_api_version(&self, api_key: i16) -> ApiVersion {
        self.get_state_ref().api_versions_map
            .as_ref()
            .unwrap()
            .get(&api_key)
            .unwrap()
            .clone()
    }
}
