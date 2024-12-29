#[tokio::main]
async fn main() {
    for i in 1..=10 {
        let f = async {
            println!("{}", i);
        };
        f.await;
    }
}
