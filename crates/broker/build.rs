pub fn main() {
    if cfg!(target_os = "windows") {
        let no_icon = std::env::var("NO_FIBRIL_ICON").unwrap_or_else(|_| "false".into());
        if !["1", "true", "yes"].contains(&no_icon.as_str()) {
            let mut res = winres::WindowsResource::new();
            res.set_icon("../../icon.ico");
            res.compile().unwrap();
        }
    }
}
