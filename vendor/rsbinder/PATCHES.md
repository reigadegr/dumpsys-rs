# Local Patches

Source: crates.io `rsbinder` 0.10.0, licensed under Apache-2.0.

`src/thread_state.rs`: accept a nullable interface descriptor in
`INTERFACE_TRANSACTION` replies. Android dump-only Binder services such as
`battery` can have no attached interface. Represent that null descriptor as
an empty string while continuing to propagate transport and malformed-Parcel
errors. Other non-nullable String fields retain their existing validation.

The Android regression test is
`op_charge_controller/crates/scheduler/tests/battery_service.rs`; it binds and
reads the real battery service without changing its state. It is ignored by
default because it requires a running Android system and dump permission.
