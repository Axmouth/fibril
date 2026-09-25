// Current documentation navigation. Historical snapshots keep their saved sidebars.
// Starlight opens the groups containing the current page automatically.
export const sidebar = [
  {
    label: "Start here",
    collapsed: false,
    items: [
      {
        label: "Overview",
        slug: "overview",
      },
      {
        label: "Quickstart",
        slug: "quickstart",
      },
      {
        label: "Client usage",
        slug: "clients",
      },
      {
        label: "Configuration",
        slug: "configuration",
      },
    ],
  },
  {
    label: "Messaging",
    collapsed: true,
    items: [
      {
        label: "Core model",
        slug: "concepts/core-model",
      },
      {
        label: "Plexus streams",
        slug: "concepts/plexus-streams",
      },
      {
        label: "Consumer groups",
        slug: "concepts/consumer-groups",
      },
    ],
  },
  {
    label: "Clustering and reliability",
    collapsed: true,
    items: [
      {
        label: "Clustering and coordination",
        slug: "concepts/clustering",
      },
      {
        label: "Replication",
        slug: "reliability/replication",
      },
      {
        label: "Failover and recovery",
        slug: "reliability/recovery-sealing",
      },
      {
        label: "Delivery guarantees",
        slug: "reliability/semantics",
      },
      {
        label: "Reconnects",
        slug: "reliability/reconnects",
      },
      {
        label: "Retries and delays",
        slug: "reliability/retries-delays",
      },
      {
        label: "Dead lettering",
        slug: "reliability/dead-lettering",
      },
    ],
  },
  {
    label: "Operations",
    collapsed: true,
    items: [
      {
        label: "Deployment",
        slug: "deployment/source",
      },
      {
        label: "Setting up a cluster",
        slug: "deployment/cluster",
      },
      {
        label: "Admin dashboard",
        slug: "admin-dashboard",
      },
      {
        label: "Dashboard demo",
        link: "/dashboard-demo/",
      },
      {
        label: "Monitoring",
        slug: "deployment/monitoring",
      },
      {
        label: "Failure modes",
        slug: "reliability/failure-modes",
      },
      {
        label: "Recovery quarantine",
        slug: "reliability/recovery-quarantine",
      },
    ],
  },
  {
    label: "Performance",
    collapsed: true,
    items: [
      {
        label: "Benchmarks",
        slug: "benchmarks",
      },
      {
        label: "Backpressure",
        slug: "concepts/backpressure",
      },
      {
        label: "Many idle queues",
        slug: "concepts/many-idle-queues",
      },
    ],
  },
  {
    label: "Project",
    collapsed: true,
    items: [
      {
        label: "Project status",
        slug: "status",
      },
      {
        label: "Implemented surface",
        slug: "implemented-surface",
      },
      {
        label: "Roadmap",
        slug: "roadmap",
      },
    ],
  },
  {
    label: "Development",
    collapsed: true,
    items: [
      {
        label: "Design and configuration",
        collapsed: true,
        items: [
          {
            label: "Configuration policy",
            slug: "development/config-policy",
          },
          {
            label: "Configuration design",
            slug: "development/config-design",
          },
          {
            label: "Metadata policy",
            slug: "development/metadata-policy",
          },
          {
            label: "TLS and authentication",
            slug: "development/security-internals",
          },
          {
            label: "Idle queue internals",
            slug: "development/idle-queue-internals",
          },
        ],
      },
      {
        label: "Coordination and recovery",
        collapsed: true,
        items: [
          {
            label: "Partition routing",
            slug: "development/partition-routing",
          },
          {
            label: "Live routing and cutover",
            slug: "development/live-routing-and-cutover",
          },
          {
            label: "Coordination internals",
            slug: "development/coordination-internals",
          },
          {
            label: "Reconnection grace",
            slug: "development/reconnection-grace",
          },
          {
            label: "Replication design",
            slug: "development/replication-design",
          },
          {
            label: "Recovery quarantine internals",
            slug: "development/recovery-internals",
          },
          {
            label: "Failover plan",
            slug: "development/failover-plan",
          },
        ],
      },
      {
        label: "Testing and investigations",
        collapsed: true,
        items: [
          {
            label: "Optimization and bug notes",
            slug: "development/engineering-notes",
          },
          {
            label: "Optimization log",
            slug: "development/optimization-log",
          },
          {
            label: "Deterministic simulation",
            slug: "development/deterministic-simulation",
          },
        ],
      },
      {
        label: "Contributing",
        collapsed: true,
        items: [
          {
            label: "Documentation style",
            slug: "development/docs-writing",
          },
          {
            label: "Versioning and releasing",
            slug: "development/releasing",
          },
        ],
      },
    ],
  },
];
