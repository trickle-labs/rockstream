//! The single `rockstream` binary.
//!
//! Every node role is a flag on this one binary. At v0.1 it runs an embedded
//! no-op node; see [`rockstream_cli`] for the command implementations.

use std::process::ExitCode;

use clap::Parser;
use rockstream_cli::cli_args::*;
use rockstream_cli::output::OutputFormat;
use rockstream_cli::transport::{CatalogClient, ClientIdentity, ControlClient, StorageClient};
use rockstream_cli::{
    run_audit_query, run_audit_tail, run_checkpoint_export, run_checkpoint_list,
    run_checkpoint_restore, run_checkpoint_show, run_cluster_quotas, run_cluster_status,
    run_cluster_workers_drain, run_cluster_workers_list, run_cluster_workers_status,
    run_completions, run_config_print_effective, run_config_validate, run_debug_arrangement,
    run_demo, run_doctor, run_explain_view, run_format_migrate, run_init, run_manifest_validate,
    run_qualify, run_resource_cluster, run_resource_usage, run_schema_create, run_schema_drop,
    run_schema_evolution_history, run_schema_evolution_status, run_schema_list, run_schema_show,
    run_shard_list, run_shard_migrate, run_source_drop, run_source_list, run_source_pause,
    run_source_resume, run_source_show, run_sql_compile, run_start, run_support_bundle,
    run_view_list, run_view_pause, run_view_query, run_view_resume, run_view_show, run_view_status,
    run_view_subscribe, run_workload_alter, run_workload_create, run_workload_drop,
    run_workload_list, run_workload_show, DemoOptions, DoctorOptions, InitOptions, StartOptions,
};
use rockstream_types::acl::Role;
use rockstream_types::config_resolver::CliConfigOverrides;
use rockstream_types::topology::{WorkerCapabilities, WorkerLocation};

fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let identity = cli_identity(&cli);
    let format = cli.effective_output_format();

    match cli.command {
        Command::Migrate { from, to, storage } => {
            handle_result(run_format_migrate(format, from, to, &storage), format)
        }
        Command::Start {
            storage,
            role,
            control,
            auth,
            host_id,
            worker_id,
            availability_zone,
            metrics_addr,
            listen,
            webhook_listen,
            raft_peers,
            raft_node_id,
            raft_bind,
            raft_bootstrap,
            daemon,
            control_bind,
            control_shared_storage,
            query_time_shard_dirs,
            min_epoch_ms,
            checkpoint_retention_count,
            state_budget_gb,
            exchange_direct_threshold_bytes,
            exchange_spill_threshold_mb,
            exchange_domain_size,
            exchange_force_durable,
            same_host_shm_segment_bytes,
            same_host_shm_segments_per_peer,
            max_exchange_compression_states,
            shutdown_timeout_secs,
        } => {
            let overrides = CliConfigOverrides {
                min_epoch_ms,
                checkpoint_retention_count,
                state_budget_gb,
                exchange_direct_threshold_bytes,
                exchange_spill_threshold_mb,
                exchange_domain_size,
                exchange_force_durable: if exchange_force_durable {
                    Some(true)
                } else {
                    None
                },
                same_host_shm_segment_bytes,
                same_host_shm_segments_per_peer,
                max_exchange_compression_states,
                webhook_listen_addr: webhook_listen,
                tls_cert_path: cli.tls_cert_path.clone(),
                tls_key_path: cli.tls_key_path.clone(),
                tls_ca_cert_path: cli.tls_ca_cert_path.clone(),
                internal_tls_cert_path: cli.internal_tls_cert_path.clone(),
                internal_tls_key_path: cli.internal_tls_key_path.clone(),
                internal_tls_ca_cert_path: cli.internal_tls_ca_cert_path.clone(),
                shutdown_timeout_secs,
            };
            let config = match rockstream_types::config_resolver::ConfigResolver::resolve(
                None, &overrides,
            ) {
                Ok(r) => r.config,
                Err(e) => {
                    let err = rockstream_cli::CliError::new(
                        rockstream_types::error_code::RS_0002,
                        format!("configuration resolution failed: {e}"),
                        "Check configuration files, environment variables, and CLI flags.",
                    );
                    eprintln!("{}", rockstream_cli::output::render_error(&err, format));
                    return ExitCode::FAILURE;
                }
            };

            let opts = StartOptions {
                storage,
                role,
                control,
                auth_mode: auth,
                worker_location: WorkerLocation::new(
                    host_id
                        .or_else(|| std::env::var("HOSTNAME").ok())
                        .unwrap_or_default(),
                    availability_zone
                        .or_else(|| std::env::var("ROCKSTREAM_AVAILABILITY_ZONE").ok())
                        .unwrap_or_default(),
                ),
                worker_capabilities: WorkerCapabilities {
                    same_host_arrow_shm_v1: true,
                    shuffle_codec_v1: true,
                    checkpoint_manifest_codec_v1: true,
                },
                config,
                metrics_addr,
                listen_addr: Some(listen),
                raft_peers,
                raft_node_id,
                raft_bind,
                raft_bootstrap,
                daemon,
                worker_id,
                control_bind,
                control_shared_storage,
                query_time_shard_dirs,
                shutdown_timeout_secs,
            };
            match run_start(&opts) {
                Ok(outcome) => {
                    tracing::info!(
                        audit = %outcome.audit_path.display(),
                        bundle = %outcome.bundle_path.display(),
                        events = outcome.events_written,
                        "rockstream: node stopped cleanly"
                    );
                    ExitCode::SUCCESS
                }
                Err(err) => {
                    eprintln!("{}", rockstream_cli::output::render_error(&err, format));
                    ExitCode::FAILURE
                }
            }
        }
        Command::View { command } => {
            let identity = identity.clone();
            let mut catalog = CatalogClient::new(identity);
            let res = match command {
                ViewCommand::List => run_view_list(format, &catalog),
                ViewCommand::Show { name } => run_view_show(format, &catalog, &name),
                ViewCommand::Status { name } => run_view_status(format, &catalog, name.as_deref()),
                ViewCommand::Pause { name, yes } => {
                    run_view_pause(format, &mut catalog, &name, yes)
                }
                ViewCommand::Resume { name } => run_view_resume(format, &mut catalog, &name),
                ViewCommand::Query { name, limit } => {
                    run_view_query(format, &catalog, &name, limit)
                }
                ViewCommand::Subscribe {
                    name,
                    from_epoch,
                    snapshot,
                } => run_view_subscribe(format, &catalog, &name, from_epoch, snapshot),
            };
            handle_result(res, format)
        }
        Command::Source { command } => {
            let identity = identity.clone();
            let mut catalog = CatalogClient::new(identity);
            let res = match command {
                SourceCommand::List => run_source_list(format, &catalog),
                SourceCommand::Show { name } => run_source_show(format, &catalog, &name),
                SourceCommand::Pause { name } => run_source_pause(format, &mut catalog, &name),
                SourceCommand::Resume { name } => run_source_resume(format, &mut catalog, &name),
                SourceCommand::Drop { name, yes } => {
                    run_source_drop(format, &mut catalog, &name, yes)
                }
            };
            handle_result(res, format)
        }
        Command::Schema { command } => {
            let identity = identity.clone();
            let mut catalog = CatalogClient::new(identity);
            let res = match command {
                SchemaCommand::List => run_schema_list(format, &catalog),
                SchemaCommand::Show { name } => run_schema_show(format, &catalog, &name),
                SchemaCommand::Create { name, columns } => {
                    run_schema_create(format, &mut catalog, &name, columns.as_deref())
                }
                SchemaCommand::Drop { name, yes } => {
                    run_schema_drop(format, &mut catalog, &name, yes)
                }
            };
            handle_result(res, format)
        }
        Command::Workload { command } => {
            let identity = identity.clone();
            let mut catalog = CatalogClient::new(identity);
            let res = match command {
                WorkloadCommand::List => run_workload_list(format, &catalog),
                WorkloadCommand::Show { name } => run_workload_show(format, &catalog, &name),
                WorkloadCommand::Create {
                    name,
                    priority,
                    freshness_slo_ms,
                    memory_limit,
                    max_parallelism,
                } => run_workload_create(
                    format,
                    &mut catalog,
                    &name,
                    priority,
                    freshness_slo_ms,
                    memory_limit,
                    max_parallelism,
                ),
                WorkloadCommand::Alter {
                    name,
                    priority,
                    freshness_slo_ms,
                    memory_limit,
                    max_parallelism,
                } => run_workload_alter(
                    format,
                    &mut catalog,
                    &name,
                    priority,
                    freshness_slo_ms,
                    memory_limit,
                    max_parallelism,
                ),
                WorkloadCommand::Drop { name, yes } => {
                    run_workload_drop(format, &mut catalog, &name, yes)
                }
            };
            handle_result(res, format)
        }
        Command::Cluster { ref command } => {
            let control = make_control_client(&cli, None);
            match command {
                ClusterCommand::Status => {
                    handle_result(run_cluster_status(format, &control), format)
                }
                ClusterCommand::Quotas => {
                    handle_result(run_cluster_quotas(format, &control), format)
                }
                ClusterCommand::Workers {
                    command: WorkerCommand::List,
                } => handle_result(run_cluster_workers_list(format, &control), format),
                ClusterCommand::Workers {
                    command: WorkerCommand::Status { worker_id },
                } => handle_result(
                    run_cluster_workers_status(format, &control, *worker_id),
                    format,
                ),
                ClusterCommand::Workers {
                    command:
                        WorkerCommand::Drain {
                            control: ctrl_addr,
                            worker_id,
                            yes,
                        },
                } => {
                    let control_client = if let Some(addr) = ctrl_addr {
                        make_control_client(&cli, Some(addr.clone()))
                    } else {
                        control
                    };
                    handle_result(
                        run_cluster_workers_drain(format, &control_client, *worker_id, *yes),
                        format,
                    )
                }
            }
        }
        Command::Shard { ref command } => {
            let control = make_control_client(&cli, None);
            let res = match command {
                ShardCommand::List => run_shard_list(format, &control),
                ShardCommand::Migrate { shard_id, to, yes } => {
                    run_shard_migrate(format, &control, *shard_id, *to, *yes)
                }
            };
            handle_result(res, format)
        }
        Command::Checkpoint { command } => {
            let storage = StorageClient::with_identity(identity.clone());
            let storage_path = cli
                .storage_dir
                .unwrap_or_else(|| std::path::PathBuf::from("."));
            let res = match command {
                CheckpointCommand::List => run_checkpoint_list(format, &storage, &storage_path),
                CheckpointCommand::Show { checkpoint_id } => {
                    run_checkpoint_show(format, &storage, checkpoint_id, &storage_path)
                }
                CheckpointCommand::Export { destination } => {
                    run_checkpoint_export(format, &storage, &storage_path, &destination)
                }
                CheckpointCommand::Restore {
                    source,
                    storage: target,
                    yes,
                } => run_checkpoint_restore(format, &storage, &storage_path, &source, &target, yes),
            };
            handle_result(res, format)
        }
        Command::Support { command } => {
            let storage = StorageClient::with_identity(identity.clone());
            let storage_path = cli
                .storage_dir
                .unwrap_or_else(|| std::path::PathBuf::from("."));
            let res = match command {
                SupportCommand::Bundle { view, since, out } => run_support_bundle(
                    format,
                    &storage,
                    &storage_path,
                    view.as_deref(),
                    since.as_deref(),
                    out.as_deref(),
                ),
                SupportCommand::Diagnose {
                    code,
                    correlation_id,
                    out,
                } => rockstream_cli::run_support_diagnose(
                    format,
                    &storage,
                    &storage_path,
                    code.as_deref(),
                    correlation_id.as_deref(),
                    out.as_deref(),
                ),
            };
            handle_result(res, format)
        }
        Command::Resource { command } => {
            let identity = identity.clone();
            let catalog = CatalogClient::new(identity);
            let res = match command {
                ResourceCommand::Usage { workload } => {
                    run_resource_usage(format, &catalog, workload.as_deref())
                }
                ResourceCommand::Cluster => run_resource_cluster(format, &catalog),
            };
            handle_result(res, format)
        }
        Command::SchemaEvolution { command } => {
            let identity = identity.clone();
            let catalog = CatalogClient::new(identity);
            let res = match command {
                SchemaEvolutionCommand::Status => run_schema_evolution_status(format, &catalog),
                SchemaEvolutionCommand::History => run_schema_evolution_history(format, &catalog),
            };
            handle_result(res, format)
        }
        Command::Audit { command } => {
            let storage = StorageClient::with_identity(identity.clone());
            let storage_path = cli
                .storage_dir
                .unwrap_or_else(|| std::path::PathBuf::from("."));
            let res = match command {
                AuditCommand::Tail { max } => run_audit_tail(format, &storage, &storage_path, max),
                AuditCommand::Query { filter, max } => {
                    run_audit_query(format, &storage, &storage_path, filter.as_deref(), max)
                }
            };
            handle_result(res, format)
        }
        Command::Explain {
            view,
            estimate,
            op_ids,
        } => {
            let catalog = CatalogClient::with_defaults();
            handle_result(
                run_explain_view(format, &catalog, &view, estimate, op_ids),
                format,
            )
        }
        Command::Sql { query } => handle_result(run_sql_compile(format, &query), format),
        Command::Debug { command } => {
            let catalog = CatalogClient::with_defaults();
            let res = match command {
                DebugCommand::Arrangement {
                    view,
                    op_id,
                    key,
                    epoch,
                } => run_debug_arrangement(format, &catalog, &view, &op_id, &key, epoch),
            };
            handle_result(res, format)
        }
        Command::Version { json } => {
            let id = rockstream_types::candidate_identity::CandidateIdentity::current();
            if cli.json || json || format == OutputFormat::Json {
                println!("{}", id.to_json().unwrap_or_default());
            } else {
                println!("{}", id.display_text());
            }
            ExitCode::SUCCESS
        }
        Command::Manifest { command } => match command {
            ManifestCommand::Validate { path, base_dir } => handle_result(
                run_manifest_validate(format, &path, base_dir.as_deref()),
                format,
            ),
        },
        Command::Qualify {
            check_prerequisites,
            suite,
            output,
        } => handle_result(
            run_qualify(
                format,
                check_prerequisites,
                suite.as_deref(),
                output.as_deref(),
            ),
            format,
        ),
        Command::Config { command } => match command {
            ConfigCommand::Validate {
                file,
                strict,
                check_files,
            } => handle_result(
                run_config_validate(format, file.as_deref(), strict, check_files),
                format,
            ),
            ConfigCommand::PrintEffective {
                file,
                show_origins,
                min_epoch_ms,
                checkpoint_retention_count,
                state_budget_gb,
                exchange_direct_threshold_bytes,
                exchange_spill_threshold_mb,
                exchange_domain_size,
                exchange_force_durable,
                same_host_shm_segment_bytes,
                same_host_shm_segments_per_peer,
                max_exchange_compression_states,
                webhook_listen,
            } => {
                let overrides = CliConfigOverrides {
                    min_epoch_ms,
                    checkpoint_retention_count,
                    state_budget_gb,
                    exchange_direct_threshold_bytes,
                    exchange_spill_threshold_mb,
                    exchange_domain_size,
                    exchange_force_durable: if exchange_force_durable {
                        Some(true)
                    } else {
                        None
                    },
                    same_host_shm_segment_bytes,
                    same_host_shm_segments_per_peer,
                    max_exchange_compression_states,
                    webhook_listen_addr: webhook_listen,
                    tls_cert_path: cli.tls_cert_path.clone(),
                    tls_key_path: cli.tls_key_path.clone(),
                    tls_ca_cert_path: cli.tls_ca_cert_path.clone(),
                    internal_tls_cert_path: cli.internal_tls_cert_path.clone(),
                    internal_tls_key_path: cli.internal_tls_key_path.clone(),
                    internal_tls_ca_cert_path: cli.internal_tls_ca_cert_path.clone(),
                    shutdown_timeout_secs: None,
                };
                handle_result(
                    run_config_print_effective(format, file.as_deref(), show_origins, &overrides),
                    format,
                )
            }
        },
        Command::Completions { shell } => handle_result(run_completions(shell), format),
        Command::Init {
            name,
            template,
            dir,
            force,
        } => {
            let opts = InitOptions {
                name,
                template,
                dir,
                force,
            };
            handle_result(run_init(format, &opts), format)
        }
        Command::Demo {
            scenario,
            storage,
            listen,
            keep,
            step_delay_ms,
        } => {
            let opts = DemoOptions {
                scenario,
                storage: storage.or_else(|| cli.storage_dir.clone()),
                listen: Some(listen),
                keep,
                step_delay_ms,
            };
            handle_result(run_demo(format, &opts), format)
        }
        Command::Doctor {
            config,
            storage,
            control,
            gateway,
            deep,
            include_docker,
            timeout,
        } => {
            let opts = DoctorOptions {
                config_path: config,
                storage: storage.or_else(|| {
                    cli.storage_dir
                        .as_ref()
                        .map(|p| p.to_string_lossy().to_string())
                }),
                control: control.or_else(|| cli.control.clone()),
                gateway,
                deep,
                include_docker,
                timeout: std::time::Duration::from_secs(timeout),
                tls_cert_path: cli.tls_cert_path.clone(),
                tls_key_path: cli.tls_key_path.clone(),
                tls_ca_cert_path: cli.tls_ca_cert_path.clone(),
                internal_tls_cert_path: cli.internal_tls_cert_path.clone(),
                internal_tls_key_path: cli.internal_tls_key_path.clone(),
                internal_tls_ca_cert_path: cli.internal_tls_ca_cert_path.clone(),
            };
            handle_result(run_doctor(format, &opts), format)
        }
    }
}

fn make_control_client(cli: &Cli, override_addr: Option<String>) -> ControlClient {
    let mut identity = cli_identity(cli);
    if let Some(ref p) = cli.tls_cert_path {
        identity = identity.with_cert(p.clone());
    }
    let control_addr = override_addr.or_else(|| cli.control.clone());
    let mut client = ControlClient::new(control_addr, identity);
    if cli.tls_cert_path.is_some()
        || cli.tls_key_path.is_some()
        || cli.tls_ca_cert_path.is_some()
        || cli.internal_tls_cert_path.is_some()
        || cli.internal_tls_key_path.is_some()
        || cli.internal_tls_ca_cert_path.is_some()
    {
        let cert_path = cli
            .internal_tls_cert_path
            .clone()
            .or_else(|| cli.tls_cert_path.clone());
        let key_path = cli
            .internal_tls_key_path
            .clone()
            .or_else(|| cli.tls_key_path.clone());
        let ca_cert_path = cli
            .internal_tls_ca_cert_path
            .clone()
            .or_else(|| cli.tls_ca_cert_path.clone());
        client = client.with_internal_tls(rockstream_types::identity::InternalTlsConfig {
            cert_path,
            key_path,
            ca_cert_path,
            client_auth_required: true,
            reload_enabled: false,
        });
    }
    client
}

fn cli_identity(cli: &Cli) -> ClientIdentity {
    let role = match cli.identity_role.as_str() {
        "admin" => Role::Admin,
        "pipeline-owner" => Role::PipelineOwner,
        _ => Role::Viewer,
    };
    let mut identity = ClientIdentity::new(cli.identity_user.clone()).with_role(role);
    if let Some(cert_path) = &cli.tls_cert_path {
        identity = identity.with_cert(cert_path.clone());
    }
    identity
}

fn handle_result(res: Result<String, rockstream_cli::CliError>, format: OutputFormat) -> ExitCode {
    match res {
        Ok(out) => {
            println!("{out}");
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("{}", rockstream_cli::output::render_error(&err, format));
            ExitCode::FAILURE
        }
    }
}
