# Rivet desktop

This is the Tauri client for the shared Rivet engine. The native shell starts
the headless server on loopback using the OS application-data directory for
SQLite state; the React UI consumes the same versioned REST/WebSocket API as a
remote client.

```sh
npm install
npm run build
npm run tauri dev
```

The UI intentionally reports an empty workspace until a project is connected.
It does not fabricate builds or logs when the local engine is unavailable.

The packaged shell binds the embedded engine to an ephemeral loopback port and
passes that origin to the UI at startup. API calls retry briefly while the
engine is becoming ready, so a normal startup race is not shown as an
unrecoverable connection failure.
