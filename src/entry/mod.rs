mod traits;
pub use traits::FileAttr;


#[cfg(unix)]
mod unix;
#[cfg(unix)]
pub use unix::Entry;


#[cfg(not(unix))]
mod generic;
#[cfg(not(unix))]
pub use generic::Entry;


#[cfg(test)]
mod tests {
    use super::Entry;
    use super::FileAttr;

    #[test]
    fn from_regular_path() {
        let e = Entry::from_path("files/softlink/original");
        assert!(e.is_ok());
        assert!(e.unwrap().is_some());
    }
    #[test]
    fn from_link_path() {
        let e = Entry::from_path("files/softlink/original_link");
        assert!(e.is_ok());
        assert!(e.unwrap().is_none());
    }
    #[test]
    fn from_dir_path() {
        let e = Entry::from_path("files/softlink");
        assert!(e.is_ok());
        assert!(e.unwrap().is_none());
    }
    #[test]
    fn from_nonexist_path() {
        let e = Entry::from_path("files/nonexist-path");
        assert!(e.is_err());
    }

    #[test]
    fn fileattr_interface() {
        // Interface check
        let e = Entry::from_path("files/softlink/original");
        assert!(e.is_ok());
        let e = e.unwrap();
        assert!(e.is_some());
        let e = e.unwrap();

        assert!(e.path().exists());
        assert!(0 < e.len());
    }
}
