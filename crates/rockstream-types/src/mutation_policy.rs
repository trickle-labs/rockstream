use crate::acl::Role;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutationSpec {
    pub operation: &'static str,
    pub minimum_role: Role,
    pub audit_action: &'static str,
}

pub const CLI_MUTATION_POLICY: &[MutationSpec] = &[
    MutationSpec {
        operation: "view pause",
        minimum_role: Role::PipelineOwner,
        audit_action: "view.pause",
    },
    MutationSpec {
        operation: "view resume",
        minimum_role: Role::PipelineOwner,
        audit_action: "view.resume",
    },
    MutationSpec {
        operation: "source pause",
        minimum_role: Role::PipelineOwner,
        audit_action: "source.pause",
    },
    MutationSpec {
        operation: "source resume",
        minimum_role: Role::PipelineOwner,
        audit_action: "source.resume",
    },
    MutationSpec {
        operation: "source drop",
        minimum_role: Role::Admin,
        audit_action: "source.drop",
    },
    MutationSpec {
        operation: "schema create",
        minimum_role: Role::PipelineOwner,
        audit_action: "schema.create",
    },
    MutationSpec {
        operation: "schema drop",
        minimum_role: Role::Admin,
        audit_action: "schema.drop",
    },
    MutationSpec {
        operation: "workload create",
        minimum_role: Role::Admin,
        audit_action: "workload.create",
    },
    MutationSpec {
        operation: "workload alter",
        minimum_role: Role::Admin,
        audit_action: "workload.alter",
    },
    MutationSpec {
        operation: "workload drop",
        minimum_role: Role::Admin,
        audit_action: "workload.drop",
    },
    MutationSpec {
        operation: "cluster workers drain",
        minimum_role: Role::Admin,
        audit_action: "cluster.workers.drain",
    },
    MutationSpec {
        operation: "shard migrate",
        minimum_role: Role::Admin,
        audit_action: "shard.migrate",
    },
    MutationSpec {
        operation: "checkpoint export",
        minimum_role: Role::Admin,
        audit_action: "checkpoint.export",
    },
    MutationSpec {
        operation: "checkpoint restore",
        minimum_role: Role::Admin,
        audit_action: "checkpoint.restore",
    },
    MutationSpec {
        operation: "support bundle",
        minimum_role: Role::Admin,
        audit_action: "support.bundle",
    },
];

pub const PGWIRE_MUTATION_POLICY: &[MutationSpec] = &[
    MutationSpec {
        operation: "CREATE VIEW",
        minimum_role: Role::PipelineOwner,
        audit_action: "create_view",
    },
    MutationSpec {
        operation: "REFRESH MATERIALIZED VIEW",
        minimum_role: Role::PipelineOwner,
        audit_action: "refresh_materialized_view",
    },
    MutationSpec {
        operation: "CREATE TABLE",
        minimum_role: Role::PipelineOwner,
        audit_action: "create_table",
    },
    MutationSpec {
        operation: "CREATE SINK",
        minimum_role: Role::PipelineOwner,
        audit_action: "create_sink",
    },
    MutationSpec {
        operation: "CREATE SOURCE",
        minimum_role: Role::PipelineOwner,
        audit_action: "create_source",
    },
    MutationSpec {
        operation: "ALTER SOURCE PAUSE",
        minimum_role: Role::PipelineOwner,
        audit_action: "alter_source.pause",
    },
    MutationSpec {
        operation: "ALTER SOURCE RESUME",
        minimum_role: Role::PipelineOwner,
        audit_action: "alter_source.resume",
    },
    MutationSpec {
        operation: "ALTER SOURCE",
        minimum_role: Role::Admin,
        audit_action: "alter_source",
    },
    MutationSpec {
        operation: "DROP SOURCE",
        minimum_role: Role::Admin,
        audit_action: "alter_source.drop",
    },
    MutationSpec {
        operation: "CREATE SECRET",
        minimum_role: Role::Admin,
        audit_action: "create_secret",
    },
    MutationSpec {
        operation: "ALTER SECRET",
        minimum_role: Role::Admin,
        audit_action: "alter_secret",
    },
    MutationSpec {
        operation: "DROP SECRET",
        minimum_role: Role::Admin,
        audit_action: "drop_secret",
    },
    MutationSpec {
        operation: "CREATE INDEX",
        minimum_role: Role::Admin,
        audit_action: "create_index",
    },
    MutationSpec {
        operation: "DROP INDEX",
        minimum_role: Role::Admin,
        audit_action: "drop_index",
    },
    MutationSpec {
        operation: "REBUILD INDEX",
        minimum_role: Role::Admin,
        audit_action: "rebuild_index",
    },
    MutationSpec {
        operation: "MARK INDEX",
        minimum_role: Role::Admin,
        audit_action: "mark_index",
    },
    MutationSpec {
        operation: "CREATE WORKLOAD",
        minimum_role: Role::Admin,
        audit_action: "create_workload",
    },
    MutationSpec {
        operation: "ALTER WORKLOAD",
        minimum_role: Role::Admin,
        audit_action: "alter_workload",
    },
    MutationSpec {
        operation: "DROP WORKLOAD",
        minimum_role: Role::Admin,
        audit_action: "drop_workload",
    },
    MutationSpec {
        operation: "INSERT",
        minimum_role: Role::PipelineOwner,
        audit_action: "insert",
    },
    MutationSpec {
        operation: "UPDATE",
        minimum_role: Role::PipelineOwner,
        audit_action: "update",
    },
    MutationSpec {
        operation: "DELETE",
        minimum_role: Role::PipelineOwner,
        audit_action: "delete",
    },
    MutationSpec {
        operation: "COPY FROM STDIN",
        minimum_role: Role::PipelineOwner,
        audit_action: "copy_in_start",
    },
    MutationSpec {
        operation: "CREATE NAMESPACE",
        minimum_role: Role::Admin,
        audit_action: "create_namespace",
    },
];

pub fn cli_mutation_policy(operation: &str) -> Option<&'static MutationSpec> {
    CLI_MUTATION_POLICY
        .iter()
        .find(|spec| spec.operation == operation)
}

pub fn pgwire_mutation_policy(query: &str) -> Option<&'static MutationSpec> {
    let query = query
        .trim()
        .trim_end_matches(';')
        .trim()
        .to_ascii_lowercase();
    let operation = if query.starts_with("create or replace view ")
        || query.starts_with("create materialized view ")
        || query.starts_with("create view ")
    {
        "CREATE VIEW"
    } else if query.starts_with("refresh materialized view ") {
        "REFRESH MATERIALIZED VIEW"
    } else if query.starts_with("create table ") {
        "CREATE TABLE"
    } else if query.starts_with("create sink ") {
        "CREATE SINK"
    } else if query.starts_with("create source ") {
        "CREATE SOURCE"
    } else if query.starts_with("drop source ") {
        "DROP SOURCE"
    } else if query.starts_with("alter source ") {
        if query.contains(" pause") {
            "ALTER SOURCE PAUSE"
        } else if query.contains(" resume") {
            "ALTER SOURCE RESUME"
        } else {
            "ALTER SOURCE"
        }
    } else if query.starts_with("create secret ") {
        "CREATE SECRET"
    } else if query.starts_with("alter secret ") {
        "ALTER SECRET"
    } else if query.starts_with("drop secret ") {
        "DROP SECRET"
    } else if query.starts_with("create index ") {
        "CREATE INDEX"
    } else if query.starts_with("drop index ") {
        "DROP INDEX"
    } else if query.starts_with("rebuild index ") {
        "REBUILD INDEX"
    } else if query.starts_with("mark index ") {
        "MARK INDEX"
    } else if query.starts_with("create workload ") {
        "CREATE WORKLOAD"
    } else if query.starts_with("alter workload ") {
        "ALTER WORKLOAD"
    } else if query.starts_with("drop workload ") {
        "DROP WORKLOAD"
    } else if query.starts_with("insert into ") {
        "INSERT"
    } else if query.starts_with("update ") {
        "UPDATE"
    } else if query.starts_with("delete from ") {
        "DELETE"
    } else if query.starts_with("copy ") && query.contains(" from stdin") {
        "COPY FROM STDIN"
    } else if query.starts_with("create namespace ") {
        "CREATE NAMESPACE"
    } else {
        return None;
    };

    PGWIRE_MUTATION_POLICY
        .iter()
        .find(|spec| spec.operation == operation)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_covers_cli_matrix_and_wire_variants() {
        assert_eq!(CLI_MUTATION_POLICY.len(), 15);
        assert_eq!(
            pgwire_mutation_policy("ALTER SOURCE foo PAUSE")
                .unwrap()
                .minimum_role,
            Role::PipelineOwner
        );
        assert_eq!(
            pgwire_mutation_policy("DROP SECRET foo")
                .unwrap()
                .minimum_role,
            Role::Admin
        );
        assert_eq!(
            pgwire_mutation_policy("INSERT INTO foo VALUES (1)")
                .unwrap()
                .minimum_role,
            Role::PipelineOwner
        );
    }
}
