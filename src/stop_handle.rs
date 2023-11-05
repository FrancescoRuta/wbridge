#[derive(Clone)]
pub struct StopHandleSnd(tokio::sync::mpsc::Sender<()>);
pub struct StopHandleRcv(tokio::sync::mpsc::Receiver<()>);

impl StopHandleSnd {
    
    pub async fn send_stop(self) {
        let _ = self.0.send(()).await;
    }
    
}

impl StopHandleRcv {
    
    pub async fn wait(&mut self) {
        let _ = self.0.recv().await;
    }
    
}

pub fn create() -> (StopHandleSnd, StopHandleRcv) {
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    (StopHandleSnd(tx), StopHandleRcv(rx))
}