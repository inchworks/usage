<h1 align="center">Usage</h1>

## Features

This Go package records and aggregates usage statistics for an internet service, such as a web server.
User identities and IP addresses can be anonymised at the time of collection, so that no personal data is stored.
Therefore there should be no need under the EU GDPR to request or record users' consents for data collection.

- Server events, such as pages accessed, are counted by name and category,
- The base interval for counts can be configured as daily, or as a shorter interval such as hourly.
- Distinct identifiers and IP addresses seen daily are counted. Typically these are for signed-up users and for unknown visitors. 
- Specified points in the lifetime of the server can be recorded to assist interpretation of the statistics.

For better performance, counts are recorded in volatile storage and transferred to the database periodically.
A callback function allows counts to be cached externally, and integrated before each database save.

Data records are automatically reduced to limit the database storage used:
- Base counts are rolled up into daily counts, and discarded, after a configurable number of days.
- Daily counts are rolled up into monthly counts, and discarded, after a configurable number of days.
- Daily counts of distinct identities seen are converted to monthly averages.
- Event counts are aggregated into category counts, and discarded, after a configurable number of months.

Three techniques are provided for anonymisation:
- IPv4 addresses are anonymised by clearing the least significant 8 bits.
IPv6 addresses are anonymised by clearing the least significant 80 bits.
- For user identifiers, a random anonymised ID is generated for each distinct user.
The mapping from anonymised ID to real (database) ID is maintained in volatile memory and discarded every 24 hours.
This scheme has the disadvantage that restarting the server will cause later visits to be counted a second time.
- An alternative scheme records the real (database) ID for each user to count daily visits. The records are deleted every 24 hours. The disadvantage is that they are available to be copied into database and server backups.

For examples of use, see https://github.com/inchworks/picinch.

## Limitations

These limitations could be improved on request:
- The daily interval is aligned to UTC.
- Up to 10 minutes of data are volatile and will be lost on server failure. This interval is not configurable.
- Event names are assumed to be unique across categories.

## Contributing

This is work in progress, and likely to change.
So I can only accept pull requests for minor fixes or improvements to existing facilities.
Please open issues to discuss new features.