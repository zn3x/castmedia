pub use reqwest;

pub struct Server {
    pub child: tokio::process::Child
}

impl Drop for Server {
    fn drop(&mut self) {
        _ = self.child.start_kill();
    }
}

pub async fn spawn_server(test_dir: &str, conf: &str, conf_name: &str) -> Server {
    let conf = conf.to_owned();

    let conf_file = format!("{}/{}", test_dir, conf_name);

    tokio::fs::write(&conf_file, conf).await
        .expect("Failed to write config file");
    
    let server = tokio::process::Command::new("cargo")
        .args([
              "run",
              "--",
              "--unsafe-password",
              &conf_file
        ])
        .spawn()
        .expect("failed to start castmedia");

    Server {
        child: server
    }
}

pub async fn get_response(url: &str) -> reqwest::Response {
    let resp = reqwest::get(url).await;
    assert!(resp.is_ok());
    resp.unwrap()
}

pub async fn get_status_code(url: &str) -> u16 {
    let resp = get_response(url).await;
    resp.status().as_u16()
}

pub async fn spawn_source(auth: &str, addr: &str, mount: &str) -> tokio::process::Child {
    tokio::process::Command::new("ffmpeg")
        .args([
              "-loglevel", "panic",
              "-re", "-f", "lavfi",
              "-i", "sine=frequency=1000",
              "-content_type", "audio/mpeg",
              "-vn", "-f", "mp3",
              "-b:a", "320k",
              &format!("icecast://{}@{}{}", auth, addr, mount)
        ])
        .spawn()
        .expect("ffmpeg missing")
}

pub async fn spawn_listener(addr: &str, mount: &str) -> tokio::process::Child {
    tokio::process::Command::new("curl")
        .args([
              "-o", "/dev/null",
              "-L",
              "-s",
              &format!("http://{}{}", addr, mount)
        ])
        .spawn()
        .expect("curl missing")
}
