//! CLI surface contributor (DOC-001).

use crate::manifest::{
    CliCommandDescriptor, CliExitCodeDescriptor, CliOptionDescriptor, CliSurface,
};
use clap::CommandFactory;

pub struct CliContributor;

impl CliContributor {
    /// Extract CLI surface from `rockstream_cli::cli_args::Cli`.
    pub fn extract() -> CliSurface {
        let cmd = rockstream_cli::cli_args::Cli::command();
        let root = Self::extract_command(&cmd);

        let mut commands = root.subcommands;
        commands.sort_by(|a, b| a.name.cmp(&b.name));
        for cmd in &mut commands {
            cmd.sort_canonical();
        }

        CliSurface { commands }
    }

    fn extract_command(cmd: &clap::Command) -> CliCommandDescriptor {
        let name = cmd.get_name().to_string();
        let about = cmd
            .get_about()
            .map(|s| s.to_string())
            .unwrap_or_else(|| cmd.get_name().to_string());

        let mut subcommands = Vec::new();
        for sub in cmd.get_subcommands() {
            if !sub.is_hide_set() {
                subcommands.push(Self::extract_command(sub));
            }
        }
        subcommands.sort_by(|a, b| a.name.cmp(&b.name));

        let mut options = Vec::new();
        for arg in cmd.get_arguments() {
            if !arg.is_hide_set() {
                let arg_name = arg.get_id().to_string();
                let short = arg.get_short();
                let long = arg.get_long().map(|s| s.to_string());
                let help = arg
                    .get_help()
                    .map(|s| s.to_string())
                    .unwrap_or_else(String::new);
                let required = arg.is_required_set();
                let value_name = arg
                    .get_value_names()
                    .and_then(|v| v.first().map(|s| s.to_string()));
                let default_value = arg
                    .get_default_values()
                    .first()
                    .map(|v| v.to_string_lossy().to_string());
                let possible_values = arg
                    .get_possible_values()
                    .into_iter()
                    .map(|pv| pv.get_name().to_string())
                    .collect();

                options.push(CliOptionDescriptor {
                    name: arg_name,
                    short,
                    long,
                    help,
                    required,
                    value_name,
                    default_value,
                    possible_values,
                });
            }
        }
        options.sort_by(|a, b| a.name.cmp(&b.name));

        let exit_codes = vec![
            CliExitCodeDescriptor {
                code: 0,
                title: "Success".to_string(),
                description: "Command completed successfully without error".to_string(),
            },
            CliExitCodeDescriptor {
                code: 1,
                title: "Execution Error".to_string(),
                description: "Runtime failure or operation error during execution".to_string(),
            },
            CliExitCodeDescriptor {
                code: 2,
                title: "Usage Error".to_string(),
                description: "Invalid arguments, options, or flags provided to CLI".to_string(),
            },
        ];

        let mut error_codes = if matches!(name.as_str(), "security" | "preprovision-admin") {
            vec![
                "RS-0002".to_string(),
                "RS-0003".to_string(),
                "RS-0005".to_string(),
            ]
        } else {
            vec![
                "RS-0001".to_string(),
                "RS-1001".to_string(),
                "RS-1012".to_string(),
                "RS-1013".to_string(),
                "RS-2001".to_string(),
            ]
        };
        error_codes.sort();

        CliCommandDescriptor {
            name,
            about,
            subcommands,
            options,
            exit_codes,
            error_codes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::CliContributor;

    #[test]
    fn offline_security_command_documents_its_actual_error_codes() {
        let surface = CliContributor::extract();
        let security = surface
            .commands
            .iter()
            .find(|command| command.name == "security")
            .unwrap();
        let preprovision = security
            .subcommands
            .iter()
            .find(|command| command.name == "preprovision-admin")
            .unwrap();
        let expected = vec![
            "RS-0002".to_string(),
            "RS-0003".to_string(),
            "RS-0005".to_string(),
        ];

        assert_eq!(security.error_codes, expected);
        assert_eq!(preprovision.error_codes, expected);
    }
}
