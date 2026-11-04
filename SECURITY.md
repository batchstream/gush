# Security policy

## Report a vulnerability

Use GitHub's private
[Report a vulnerability](https://github.com/batchstream/gush/security/advisories/new)
form. Include the affected version or commit, reproduction steps, impact, and
relevant deployment settings with credentials and private data removed. Please
keep exploit details out of public issues while a report is being investigated.

## Versions

Check whether an issue is still present on the
[latest release](https://github.com/batchstream/gush/releases/latest). Older
releases do not have a guaranteed security backport schedule. Report suspected
vulnerabilities even if you cannot reproduce them on the latest release.

## Deployment guidance

- Flow and step inputs and outputs are compressed but not encrypted, and Temporal
  stores them in its history. Do not put secrets in them.
- Resume tokens let their holder finish a suspended step. Treat them as credentials
  when handing them to another system.
- An `http://` value in `OTEL_EXPORTER_OTLP_ENDPOINT` sends logs in plaintext. Use an
  `https://` URL when logs leave a trusted network.
