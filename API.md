## ⚡PhotonDB API Reference

This document provides a detailed reference for all commands supported by PhotonDB, including examples for both the CLI and MCP server.

---

## Commands

### 1. `ZAP`

- **_Description_**: Testing whether db connection is still alive
- **CLI Example**:
  ```sh
  ⚡photon> ZAP
  (str) ZING
  ```
- **MCP Example**:
  ```sh
  > zap
  (str) ZING
  ```

### 2. `SET`

- **_Description_**: Sets a key-value pair in the database.
- **CLI Example**:
  ```sh
  ⚡photon> set foo bar
  OK
  ```
- **MCP Example**:
  ```sh
  > set the key "foo" to "bar"
  OK / key-value is stored
  ```

---

### 3. `GET`

- **_Description_**: Retrieves the value of a key.
- **CLI Example**:
  ```sh
  ⚡photon> get foo
  (str) bar
  ```
- **MCP Example**:
  ```sh
  > what is the value of "foo"?
  (str) bar
  ```

---

### 4. `DELETE`

- **_Description_**: Deletes a key from the database.
- **CLI Example**:
  ```sh
  ⚡photon> delete foo
  1 or 0
  ```
- **MCP Example**:
  ```sh
  > delete the key "foo"
  (1) successfully deleted the key-value or (0) unsuccesful
  ```

---

### 5. `KEYS`

- **_Description_**: Lists all keys from the DB.
- **CLI Example**:
  ```sh
  ⚡photon> keys
  (arr) len=2
  (str) name
  (str) age
  (arr) end
  ```
- **MCP Example**:
  ```sh
  > list the keys
  [name,age]
  ```

---

### 6. `ZADD`

- **_Description_**: Adds a member with a score to a sorted set.
- **CLI Example**:
  ```sh
  ⚡photon> zadd myzset 1.5 alice
  (int) 1
  ```
- **MCP Example**:
  ```sh
  in progress
  ```

---

### 7. `ZSCORE`

- **_Description_**: Gets the score of a member in a sorted set.
- **CLI Example**:
  ```sh
  ⚡photon> zscore myzset alice
  (dbl) 1.5
  ```
- **MCP Example**:
  ```sh
  in progress
  ```

---

### 8. `ZREM`

- **_Description_**: Removes a member from a sorted set.
- **CLI Example**:
  ```sh
  ⚡photon> zrem myzset alice
  (int) 1
  ```
- **MCP Example**:
  ```sh
  in progress
  ```

---

### 9. `ZQUERY`

- **_Description_**: Queries a sorted set for a range of members by score and name.
  `ZQUERY (zset, min_score, min_name, offset, limit)`
- **CLI Example**:
  ```sh
  ⚡photon> zquery myzset 1.0 "" 0 10
  (arr) len=2
  (str) bob
  (str) charlie
  (arr) end
  ```
- **MCP Example**:
  ```sh
  in progress
  ```

---

### 10. `PEXPIRE`

- **_Description_**: Sets a key's time to live in milliseconds.
- **CLI Example**:
  ```sh
  ⚡photon> pexpire tempkey 1000
  (int) 1
  ```
- **MCP Example**:
  ```sh
  in progress
  ```

---

### 11. `PTTL`

- **_Description_**: Gets the remaining time to live of a key in milliseconds.
- **CLI Example**:
  ```sh
  ⚡photon> pttl tempkey
  (int) 900
  ```
- **MCP Example**:
  ```sh
  in progress
  ```

---

### Notes

- All commands are case-insensitive.

- MCP commands don’t require exact keywords but rely on correct semantics to interpret the intent.
