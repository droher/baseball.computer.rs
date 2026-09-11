# baseball.computer - The Rust Part

Rust parser for the baseball.computer database.

Pitch exports retain all pitches in a plate appearance and deduplicate only
confirmed cumulative prefixes. Compatible annotations are reconciled; reviewed
conflicts quarantine the appearance, and unexpected conflicts fail the export. See
[pitch history and validation](docs/pitch_sequence_pa_resume.md) for source
archive checks, attribution rules, and downstream regeneration requirements.

Duplicate source games use a documented, thread-count-independent selection
policy. See [the schema documentation](docs/schema.md) for account precedence,
failure behavior, and the meaning of that ordering.

See the website or the main repo for more info:

https://baseball.computer/

https://github.com/droher/baseball.computer
