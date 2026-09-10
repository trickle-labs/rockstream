//! NodeRuntime & Role Composition Integration Tests (v0.62 Slice 6 / Phase 3b).

use rockstream_cli::component::{
    Component, ControlComponent, GatewayComponent, MetricsComponent, NodeRuntime, WorkerComponent,
};
use rockstream_types::config::NodeConfig;
use rockstream_types::lifecycle::LifecycleState;

#[tokio::test]
async fn test_role_all_composes_identical_components_as_separate_roles() {
    // 1. Role "all"
    let mut config_all = NodeConfig::default();
    config_all.node.role = "all".to_string();
    let runtime_all = NodeRuntime::new(config_all).expect("runtime all creation");
    let comp_names_all: Vec<&str> = runtime_all.components().iter().map(|c| c.name()).collect();
    assert_eq!(
        comp_names_all,
        vec![
            "control",
            "worker",
            "gateway",
            "metrics",
            "connector_supervisor"
        ]
    );

    // 2. Role "control"
    let mut config_control = NodeConfig::default();
    config_control.node.role = "control".to_string();
    let runtime_control = NodeRuntime::new(config_control).expect("runtime control creation");
    let comp_names_control: Vec<&str> = runtime_control
        .components()
        .iter()
        .map(|c| c.name())
        .collect();
    assert_eq!(comp_names_control, vec!["control", "metrics"]);

    // 3. Role "worker"
    let mut config_worker = NodeConfig::default();
    config_worker.node.role = "worker".to_string();
    let runtime_worker = NodeRuntime::new(config_worker).expect("runtime worker creation");
    let comp_names_worker: Vec<&str> = runtime_worker
        .components()
        .iter()
        .map(|c| c.name())
        .collect();
    assert_eq!(comp_names_worker, vec!["worker", "metrics"]);

    // 4. Role "gateway"
    let mut config_gateway = NodeConfig::default();
    config_gateway.node.role = "gateway".to_string();
    let runtime_gateway = NodeRuntime::new(config_gateway).expect("runtime gateway creation");
    let comp_names_gateway: Vec<&str> = runtime_gateway
        .components()
        .iter()
        .map(|c| c.name())
        .collect();
    assert_eq!(comp_names_gateway, vec!["gateway", "metrics"]);

    // 5. Role "metrics"
    let mut config_metrics = NodeConfig::default();
    config_metrics.node.role = "metrics".to_string();
    let runtime_metrics = NodeRuntime::new(config_metrics).expect("runtime metrics creation");
    let comp_names_metrics: Vec<&str> = runtime_metrics
        .components()
        .iter()
        .map(|c| c.name())
        .collect();
    assert_eq!(comp_names_metrics, vec!["metrics"]);

    // Symmetry: each component in dedicated roles is represented identically in role "all"
    for name in &["control", "worker", "gateway", "metrics"] {
        assert!(
            comp_names_all.contains(name),
            "Component `{name}` in separate roles must be composed in `all`"
        );
    }
}

#[tokio::test]
async fn test_partial_startup_failure_cleans_up_started_components() {
    let mut config = NodeConfig::default();
    config.node.role = "all".to_string();

    // Injected components where Gateway fails to start
    // Order: Control (starts OK), Worker (starts OK), Gateway (fails on start)
    let components: Vec<Box<dyn Component>> = vec![
        Box::new(ControlComponent::new()),
        Box::new(WorkerComponent::new()),
        Box::new(GatewayComponent::new().with_start_failure(true)),
        Box::new(MetricsComponent::new()),
    ];

    let mut runtime = NodeRuntime::with_components(config, components);
    let mut event_rx = runtime.take_event_receiver().expect("event rx");

    let result = runtime.start().await;
    assert!(
        result.is_err(),
        "Runtime start must fail on gateway failure"
    );

    // Runtime state must transition to Fatal
    assert_eq!(runtime.tracker().state(), LifecycleState::Fatal);

    // Collect lifecycle trace events
    let mut events = Vec::new();
    while let Ok(evt) = event_rx.try_recv() {
        events.push(evt);
    }

    // Verify reverse teardown occurred:
    // Control started -> Worker started -> Gateway failed (Fatal)
    // -> Worker stopped -> Control stopped
    let comp_names_and_states: Vec<(&str, LifecycleState)> =
        events.iter().map(|e| (e.component, e.next)).collect();

    assert!(comp_names_and_states.contains(&("control", LifecycleState::Starting)));
    assert!(comp_names_and_states.contains(&("worker", LifecycleState::Starting)));
    assert!(comp_names_and_states.contains(&("gateway", LifecycleState::Fatal)));
    assert!(comp_names_and_states.contains(&("worker", LifecycleState::Stopped)));
    assert!(comp_names_and_states.contains(&("control", LifecycleState::Stopped)));

    // Metrics should NEVER have started since Gateway failed before it
    assert!(!comp_names_and_states
        .iter()
        .any(|(name, _)| *name == "metrics"));
}
