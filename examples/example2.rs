use tokio::time::{self,Duration};

#[tokio::main]
async fn main() {
    for i in 1..=10 {
        let f = async {
            time::sleep(Duration::from_secs_f32(rand::random::<f32>() * 1.0)).await;
            println!("{}", i);
        };
        f.await;
    }
}
