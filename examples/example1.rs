struct Person {
    name: Box<&'static str>, // 使用 Box<&'static str> 存储对字符串的引用
}

fn main() {
    // 创建一个静态字符串，确保它的生命周期是 'static
    let name: &'static str = "Alice"; // 直接使用静态字符串

    // 将引用存储在 Box 中
    let person = Person {
        name: Box::new(name), // 将静态字符串的引用存储在 Box 中
    };

    // 现在可以安全地使用 person.name
    println!("Person's name: {}", *person.name); // 输出: Person's name: Alice
}