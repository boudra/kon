extern crate bindgen;

#[cfg(feature = "duckdb")]
fn main() {
    use std::env;
    use std::path::PathBuf;
    let target = env::var("TARGET").unwrap();

    cc::Build::new()
        .cpp(true)
        .compiler("c++")
        .flag("-std=c++17")
        .flag("-Wno-unused-parameter")
        .include("/opt/homebrew/include")
        .file("libkon-sys/main.cpp")
        .compile("libkon-sys.a");

    println!("cargo:rustc-link-lib=static=kon-sys");
    println!("cargo:rustc-link-lib=dylib=duckdb");

    if target.contains("darwin") {
        println!("cargo:rustc-link-search=native=/opt/homebrew/lib");
    } else if target.contains("linux") {
        println!("cargo:rustc-link-lib=dylib=stdc++");
        println!("cargo:rustc-link-search=native=/usr/local/lib");
    } else {
        panic!("unsupported target");
    }

    // Tell cargo to invalidate the built crate whenever the wrapper changes
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=libkon-sys/main.h");
    println!("cargo:rerun-if-changed=libkon-sys/main.cpp");

    // The bindgen::Builder is the main entry point
    // to bindgen, and lets you build up options for
    // the resulting bindings.
    let bindings = bindgen::Builder::default()
        // The input header we would like to generate
        // bindings for.
        .clang_arg("-I/opt/homebrew/include")
        .header("libkon-sys/main.h")
        .allowlist_file(".*main.h")
        .allowlist_file(".*duckdb.h")
        .allowlist_file(".*arrow.h")
        .trust_clang_mangling(false)
        // Tell cargo to invalidate the built crate whenever any of the
        // included header files changed.
        .parse_callbacks(Box::new(bindgen::CargoCallbacks))
        // Finish the builder and generate the bindings.
        .generate()
        // Unwrap the Result and panic on failure.
        .expect("Unable to generate bindings");

    // Write the bindings to the $OUT_DIR/bindings.rs file.
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());

    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}

#[cfg(not(feature = "duckdb"))]
fn main() {}
