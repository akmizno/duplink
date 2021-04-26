mod traits;
pub use traits::{Entry, OsEntry};

#[cfg(unix)]
mod unixentry;
#[cfg(unix)]
pub use unixentry::DEntry;

#[cfg(test)]
mod tests {
    use super::DEntry;

    #[test]
    fn from_regular_path() {
        let e = DEntry::from_path("files/softlink/original");
        assert!(e.is_ok());
        assert!(e.unwrap().is_some());
    }
    #[test]
    fn from_link_path() {
        let e = DEntry::from_path("files/softlink/original_link");
        assert!(e.is_ok());
        assert!(e.unwrap().is_none());
    }
    #[test]
    fn from_dir_path() {
        let e = DEntry::from_path("files/softlink");
        assert!(e.is_ok());
        assert!(e.unwrap().is_none());
    }
    #[test]
    fn from_nonexist_path() {
        let e = DEntry::from_path("files/nonexist-path");
        assert!(e.is_err());
    }
}
