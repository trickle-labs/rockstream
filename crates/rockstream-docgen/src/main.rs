//! CLI binary for `rockstream-docgen` (DOC-001, DOC-004).

use clap::{Parser, Subcommand};
use std::fs;
use std::path::PathBuf;
use std::process;

use rockstream_docgen::{
    generate_manifest, render_reference_docs, ProductSurfaceManifest, SqlMatrixDocument,
};

#[derive(Debug, Parser)]
#[command(
    name = "rockstream-docgen",
    about = "RockStream Product Surface Generator & Drift Checker"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Generate deterministic `docs/product-surface.json` manifest.
    Generate {
        #[arg(long, default_value = "docs/product-surface.json")]
        output: PathBuf,
    },
    /// Check for drift between live code/contracts and `docs/product-surface.json`.
    Check {
        #[arg(long, default_value = "docs/product-surface.json")]
        manifest_path: PathBuf,
    },
    /// Validate `contracts/sql-type-matrix.toml`.
    ValidateSqlMatrix {
        #[arg(long, default_value = "contracts/sql-type-matrix.toml")]
        matrix_path: PathBuf,
    },
    /// Generate deterministic Markdown references from a product-surface manifest.
    GenerateReferences {
        #[arg(long, default_value = "docs/product-surface.json")]
        manifest_path: PathBuf,
        #[arg(long, default_value = "docs/reference")]
        output_dir: PathBuf,
    },
}

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Commands::Generate { output } => {
            let manifest = match generate_manifest() {
                Ok(m) => m,
                Err(err) => {
                    eprintln!("Error generating manifest: {err}");
                    process::exit(1);
                }
            };
            let json = match manifest.to_canonical_json() {
                Ok(j) => j,
                Err(err) => {
                    eprintln!("Error serializing manifest to JSON: {err}");
                    process::exit(1);
                }
            };
            if let Some(parent) = output.parent() {
                let _ = fs::create_dir_all(parent);
            }
            if let Err(err) = fs::write(&output, format!("{json}\n")) {
                eprintln!("Error writing manifest to {}: {err}", output.display());
                process::exit(1);
            }
            println!(
                "Successfully generated product surface manifest: {}",
                output.display()
            );
        }
        Commands::Check { manifest_path } => {
            let existing_content = match fs::read_to_string(&manifest_path) {
                Ok(c) => c,
                Err(err) => {
                    eprintln!(
                        "Error reading manifest at {}: {err}",
                        manifest_path.display()
                    );
                    process::exit(1);
                }
            };
            let live_manifest = match generate_manifest() {
                Ok(m) => m,
                Err(err) => {
                    eprintln!("Error generating live manifest: {err}");
                    process::exit(1);
                }
            };
            let live_json = match live_manifest.to_canonical_json() {
                Ok(j) => format!("{j}\n"),
                Err(err) => {
                    eprintln!("Error serializing live manifest: {err}");
                    process::exit(1);
                }
            };
            if existing_content.trim() != live_json.trim() {
                eprintln!(
                    "Drift detected between live code/contracts and {}",
                    manifest_path.display()
                );
                process::exit(1);
            }
            println!("Zero drift detected: manifest is in sync with live code and contracts.");
        }
        Commands::ValidateSqlMatrix { matrix_path } => {
            let content = match fs::read_to_string(&matrix_path) {
                Ok(c) => c,
                Err(err) => {
                    eprintln!("Error reading matrix at {}: {err}", matrix_path.display());
                    process::exit(1);
                }
            };
            match SqlMatrixDocument::parse(&content) {
                Ok(_) => {
                    println!("SQL type matrix is valid: {}", matrix_path.display());
                }
                Err(err) => {
                    eprintln!("SQL type matrix validation error: {err}");
                    process::exit(1);
                }
            }
        }
        Commands::GenerateReferences {
            manifest_path,
            output_dir,
        } => {
            let content = match fs::read_to_string(&manifest_path) {
                Ok(content) => content,
                Err(err) => {
                    eprintln!(
                        "Error reading manifest at {}: {err}",
                        manifest_path.display()
                    );
                    process::exit(1);
                }
            };
            let manifest: ProductSurfaceManifest = match serde_json::from_str(&content) {
                Ok(manifest) => manifest,
                Err(err) => {
                    eprintln!(
                        "Error parsing manifest at {}: {err}",
                        manifest_path.display()
                    );
                    process::exit(1);
                }
            };
            if let Err(err) = fs::create_dir_all(&output_dir) {
                eprintln!("Error creating {}: {err}", output_dir.display());
                process::exit(1);
            }
            for (name, markdown) in render_reference_docs(&manifest) {
                if let Err(err) = fs::write(output_dir.join(&name), markdown) {
                    eprintln!("Error writing {}/{}: {err}", output_dir.display(), name);
                    process::exit(1);
                }
            }
            println!(
                "Successfully generated Markdown references: {}",
                output_dir.display()
            );
        }
    }
}
