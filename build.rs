use std::process::Command; // 导入标准库中的进程模块，用于执行外部命令

fn main() {
    let build_enabled = option_env!("BUILD_PROTO") // 检查环境变量是否设置了BUILD_PROTO
        .map(|v| v == "1") // 如果设置了，则检查其值是否为"1"
        .unwrap_or(false); // 如果未设置，则返回false

    if !build_enabled {
        println!("=== Skipped compiling protos ==="); // 如果未启用编译，打印跳过编译的信息
        return; // 并退出程序
    }

    let mut config = prost_build::Config::new(); // 创建一个新的配置实例
    config.bytes(&["."]); // 匹配所有路径下的所有字段。bytes转换为bytes.Bytes
    config.type_attribute(".", "#[derive(PartialOrd)]"); // 为所有类型添加偏序关系的派生
    config
        // 设置输出目录为src/pb
        .out_dir("src/pb") 
        // 编译abi.proto文件，includes为搜索路径
        .compile_protos(&["abi.proto"], &["."]) 
        // 如果编译失败，会panic
        .unwrap(); 

    Command::new("cargo") // 创建一个新的cargo命令
        .args(&["fmt", "--", "src/*.rs"]) // 设置命令参数为格式化所有src目录下的Rust文件
        .status() // 执行命令并获取状态
        .expect("cargo fmt failed"); // 如果命令执行失败，会panic
}
