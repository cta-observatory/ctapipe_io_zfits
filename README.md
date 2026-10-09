# ctapipe_io_zfits

ctapipe io plugin for reading data in zfits files.

Supports DL0v1 data as written by ACADA and R1v1 telescope data for commissioning.
`ProtozfitsEventSource` reads subarray DL0 data, while
`ProtozfitsTelescopeEventSource` reads individual R1 or DL0 telescope streams.
Both sources are available from `ctapipe_io_zfits.source` and the package root.

This `EventSource` implementation uses the `protozfits` python wrappers to the `adh-apis`
C++ project to read data written using ProtocolBuffers into zfits.
