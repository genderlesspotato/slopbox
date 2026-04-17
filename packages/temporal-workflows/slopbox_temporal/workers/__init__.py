"""Per-theme Temporal worker entrypoints.

Each theme in slopbox maps to its own Temporal task queue and its own worker
process.  This is deliberate: themes differ in the tools and credentials
their activities need at runtime (the ``log-ops`` worker needs ES access, the
``maintenance`` worker needs SSH keys and Ansible playbooks), and running
them in one process would give the union of all those privileges to every
activity.

To add a new theme:

1. Decide on a task-queue name (lowercase, hyphen-separated).
2. Create ``slopbox_temporal/workers/<theme>.py`` with a ``main()`` that
   calls :func:`slopbox_temporal._shared.logging.configure_worker_logging`,
   connects to Temporal, and instantiates a ``Worker`` with the full set of
   workflows and activities that belong to that theme.
3. Document the queue name in the README and deploy a worker process bound
   to that queue.
"""
