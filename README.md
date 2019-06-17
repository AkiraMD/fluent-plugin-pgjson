# Fluent::Plugin::PgJson, a plugin for [Fluentd](http://fluentd.org)

Output Plugin for PostgreSQL Json Type.

<b>Json type is availble in PostgreSQL version over 9.2</b>

## Requirements

| fluent-plugin-pgjson | fluentd    | Ruby   |
|----------------------|------------|--------|
| >= 1.0.0             | >= v1.0.0  | >= 2.2 |
|  < 1.0.0             | >= v0.12.0 | >= 1.9 |

## Installation

```
$ fluent-gem install fluent-plugin-pgjson
```

## Schema

Specified table must have following schema:

| col          | type                     |
|--------------|--------------------------|
| {tag_col}    | Text                     |
| {time_col}   | Timestamp WITH TIME ZONE |
| {record_col} | Json                     |

### Example

```
CREATE TABLE fluentd (
    tag Text
    ,time Timestamptz
    ,record Json
);
```
### JSONB?

Yes! Just define a record column as JSONB type.

```
CREATE TABLE fluentd (
    tag Text
    ,time Timestamptz
    ,record Jsonb
);
```

## Configuration

### Example

```
<match **>
  @type pgjson
  host localhost
  port 5432
  sslmode require
  database fluentd
  table fluentd
  user postgres
  password postgres
  time_col time
  tag_col tag
  record_col record
  record_ext_map {
    "user_id": "uid",
    "message": "msg"
  }
</match>
```

### Parameter

* **host** (string) (optional): The hostname of PostgreSQL server
  * Default value: `localhost`.
* **port** (integer) (optional): The port of PostgreSQL server
  * Default value: `5432`.
* **sslmode** (enum) (optional): Set the sslmode to enable Eavesdropping protection/MITM protection. See [PostgreSQL Documentation](https://www.postgresql.org/docs/10/static/libpq-ssl.html) for more details.
  * Available values: disable, allow, prefer, require, verify-ca, verify-full
  * Default value: `prefer`.
* **database** (string) (required): The database name to connect
* **table** (string) (required): The table name to insert records
* **user** (string) (optional): The user name to connect database
* **password** (string) (optional): The password to connect database
* **time_col** (string) (optional): The column name for the time
  * Default value: `time`.
* **tag_col** (string) (optional): The column name for the tag
  * Default value: `tag`.
* **record_col** (string) (optional): The column name for the record
  * Default value: `record`.
* **msgpack** (bool) (optional): If true, insert records formatted as msgpack
* **encoder** (enum) (optional): JSON encoder (yajl/json)
  * Available values: yajl, json
  * Default value: `yajl`.

### record_ext_map

The `record_ext_map` configuration parameter lets you extract keys from the
incoming log entry to columns in your database table. For example, the following
would map the "user_id" value in an incoming entry to a "uid" column, and the
"message" value to the "msg" column:

```
record_ext_map {
"user_id": "uid",
"message": "msg"
}
```

Note that keys mapped using this parameter are automatically removed from the
record stored in your `record_col`. So given the above `record_ext_map` and an
incoming event like the following...

```
{
"user_id": 123,
"message": "Testing",
"foo": "bar"
}
```

You would end up with a row in your PostgreSQL table like this:

```
+------+------+------+---------+----------------+
| ts   | tag  | uid  | msg     | record         |
+------+------+------+---------+----------------+
| ...  | ...  | 123  | Testing | {"foo": "bar"} |
+------+------+------+---------+----------------+
```

## Copyright

* Copyright (c) 2014- OKUNO Akihiro
* License
    * Apache License, version 2.0
