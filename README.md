# baseball.computer - The Rust Part

Rust parser for the baseball.computer database.

Pitch exports retain all pitches in a plate appearance and deduplicate only
confirmed cumulative prefixes. Compatible annotations are reconciled; reviewed
conflicts quarantine the appearance, and unexpected conflicts fail the export. See
[pitch history and validation](docs/pitch_sequence_pa_resume.md) for source
archive checks, attribution rules, and downstream regeneration requirements.

See the website or the main repo for more info:

https://baseball.computer/

https://github.com/droher/baseball.computer
