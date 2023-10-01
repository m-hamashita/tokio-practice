use std::env;
use std::time::Duration;

use anyhow::{anyhow, Result};
use tokio::sync::oneshot;
use tokio::time::timeout;

pub async fn spawn_blocking<F, T>(
    f: F,
    timeout_duration: Option<Duration>,
) -> Result<T, anyhow::Error>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let (send, recv) = oneshot::channel();
    rayon::spawn(move || {
        let _ = send.send(f());
    });

    let default_timeout_duration = env::var("DEFAULT_SPAWN_TIMEOUT")
        .and_then(|v| v.parse().map_err(|_| env::VarError::NotPresent))
        .map_or(Duration::from_millis(150), Duration::from_millis);
    let timeout_duration = timeout_duration.unwrap_or(default_timeout_duration);
    timeout(timeout_duration, recv)
        .await
        .map_err(|_| anyhow!("spawn_blocking timeout"))?
        .map_err(anyhow::Error::from)
}


#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;

    #[rstest]
    #[case(None, Duration::from_millis(100), Ok("ok"))]
    #[case(Some(Duration::from_millis(300)), Duration::from_millis(200), Ok("ok"))]
    #[case(Some(Duration::from_millis(300)), Duration::from_millis(500), Err(anyhow!("spawn_blocking timeout")))]
    #[tokio::test]
    async fn test_spawn_blocking_with_timeout(
        #[case] timeout_duration: Option<Duration>,
        #[case] sleep_duration: Duration,
        #[case] expected: Result<&str>,
    ) {
        let actual = spawn_blocking(move || {
            std::thread::sleep(sleep_duration);
            "ok"
        }, timeout_duration).await;

        match (actual, expected) {
            (Ok(actual), Ok(expected)) => assert_eq!(actual, expected),
            (Err(actual), Err(expected)) => assert_eq!(actual.to_string(), expected.to_string()),
            _ => panic!("unexpected result"),
        }

    }
}
