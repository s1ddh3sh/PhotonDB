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

### Notes

- All commands are case-insensitive.

- MCP commands don’t require exact keywords but rely on correct semantics to interpret the intent.
