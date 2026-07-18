/// A simple protobuf message.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Msg {
    #[prost(enumeration="msg::Type", tag="1")]
    pub r#type: i32,
    #[prost(uint64, tag="2")]
    pub id: u64,
    #[prost(string, tag="3")]
    pub name: std::string::String,
    #[prost(bytes, repeated, tag="4")]
    pub paylad: ::std::vec::Vec<std::vec::Vec<u8>>,
}
pub mod msg {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Type {
        Unknown = 0,
        Put = 1,
        Get = 2,
        Del = 3,
    }
}
